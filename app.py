import io
import os
import re
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, List
import uuid

import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, send_file, jsonify, session
from unidecode import unidecode
from threading import Thread, Lock
import json


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

# simple in-memory result store per session id
RESULT_STORE: Dict[str, bytes] = {}

# background task bookkeeping (also mirrored to disk for multi-worker safety)
TASK_LOCK = Lock()
TASKS: Dict[str, Dict] = {}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
]


def rotate_headers() -> Dict[str, str]:
    agent = USER_AGENTS[int(time.time()) % len(USER_AGENTS)]
    return {"User-Agent": agent}


def clean_cnpj(raw: str) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    return digits if digits else None


def slugify(text: str) -> str:
    text = unidecode(text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text.replace(" ", "-")


def fetch_cnpj_data(cnpj: str) -> Optional[dict]:
    url = f"https://publica.cnpj.ws/cnpj/{cnpj}"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        return None
    except requests.RequestException:
        return None


def parse_api_fields(data: dict) -> Dict[str, Optional[str]]:
    if not data:
        return {}
    estabelecimento = data.get("estabelecimento") or {}
    natureza = data.get("natureza_juridica") or {}
    porte = data.get("porte") or {}

    telefone = estabelecimento.get("telefone1") or estabelecimento.get("telefone2")
    email = estabelecimento.get("email")

    return {
        "razao_social": data.get("razao_social"),
        "natureza_juridica": natureza.get("descricao"),
        "descricao_situacao_cadastral": data.get("descricao_situacao_cadastral"),
        "data_inicio_atividade": data.get("data_inicio_atividade"),
        "porte": porte.get("descricao"),
        "opcao_pelo_mei": data.get("opcao_pelo_mei"),
        "telefone": telefone,
        "email": email,
    }


def scrape_telelistas(query: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        url = f"https://www.telelistas.net/busca?q={requests.utils.quote(query)}"
        r = requests.get(url, headers=rotate_headers(), timeout=30)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")
        possible_phone = None
        phone_el = soup.find(text=re.compile(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}"))
        if phone_el:
            match = re.search(r"(\d{2})\D*(\d{4,5})\D*(\d{4})", phone_el)
            if match:
                possible_phone = "".join(match.groups())
        possible_email = None
        email_el = soup.find(text=re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"))
        if email_el:
            em = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", email_el)
            if em:
                possible_email = em.group(0)
        return possible_phone, possible_email
    except Exception:
        return None, None


def scrape_consultasocio(query: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        # First try search by company name
        search_url = f"https://www.consultasocio.com/q/{requests.utils.quote(query)}"
        r = requests.get(search_url, headers=rotate_headers(), timeout=30)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")
        # Try to find a company link
        company_link = soup.select_one('a[href*="/empresa/"]')
        if company_link and company_link.get('href'):
            detail_url = "https://www.consultasocio.com" + company_link.get('href')
            d = requests.get(detail_url, headers=rotate_headers(), timeout=30)
            if d.status_code == 200:
                dsoup = BeautifulSoup(d.text, "html.parser")
                text = dsoup.get_text(" ")
                phone = None
                email = None
                pm = re.search(r"(\(?\d{2}\)?\s?\d{4,5}-?\d{4})", text)
                if pm:
                    digits = re.findall(r"\d", pm.group(0))
                    if digits:
                        phone = "".join(digits)
                em = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                if em:
                    email = em.group(0)
                return phone, email
        return None, None
    except Exception:
        return None, None


def scrape_fallback(company_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not company_name:
        return None, None
    phone, email = scrape_telelistas(company_name)
    if not phone or not email:
        time.sleep(1)
        p2, e2 = scrape_consultasocio(company_name)
        phone = phone or p2
        email = email or e2
    return phone, email


def format_output_block(cnpj: str, fields: Dict[str, Optional[str]]) -> str:
    razao_social = (fields.get("razao_social") or "").strip()
    razao_social_upper = razao_social.upper()
    slug = slugify(razao_social)
    natureza = fields.get("natureza_juridica") or ""
    situacao = fields.get("descricao_situacao_cadastral") or ""
    data_inicio = fields.get("data_inicio_atividade") or ""
    porte = fields.get("porte") or ""
    mei_raw = fields.get("opcao_pelo_mei")
    mei = "Yes" if mei_raw else "No"
    telefone = fields.get("telefone") or ""
    email = fields.get("email") or ""

    # Adhering to required exact labels and order
    lines = [
        f"NOME NA BRADESCO: {razao_social_upper} BOM",
        "AGENCIA: ",
        "CONTA: ",
        f"NOME API DE PUXADA: {slug}",
        f"CNPJ: {cnpj}",
        f"NATUREZA: {natureza}",
        f"SITUAÇAO: {situacao} desde {data_inicio}",
        f"PORTE: {porte}",
        f"MEI: {mei}",
        f"TEL: {telefone}",
        f"EMAIL: {email}",
    ]
    return "\n".join(lines)


def process_cnpjs(cnpjs: List[str]) -> str:
    output_blocks = []
    for raw in cnpjs:
        cnpj = clean_cnpj(raw)
        if not cnpj:
            continue
        data = fetch_cnpj_data(cnpj)
        fields = parse_api_fields(data) if data else {}

        # If phone or email missing, scrape
        if not fields.get("telefone") or not fields.get("email"):
            time.sleep(1)
            s_phone, s_email = scrape_fallback(fields.get("razao_social"))
            fields["telefone"] = fields.get("telefone") or s_phone
            fields["email"] = fields.get("email") or s_email

        # Sleep to avoid rate limits
        time.sleep(1)

        output_blocks.append(format_output_block(cnpj, fields))

    return "\n\n".join(output_blocks) + "\n"


def _write_json(path: str, data: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _read_json(path: str) -> Optional[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _background_process(task_id: str, cnpjs: List[str]) -> None:
    status_path = os.path.join(OUTPUT_DIR, f"{task_id}.json")
    out_path = os.path.join(OUTPUT_DIR, f"resultado_{task_id}.txt")
    with open(out_path, "w", encoding="utf-8") as out:
        total = len(cnpjs)
        processed = 0
        state = {"status": "running", "processed": 0, "total": total, "file": out_path}
        _write_json(status_path, state)

        for raw in cnpjs:
            try:
                cnpj = clean_cnpj(raw)
                if not cnpj:
                    continue
                data = fetch_cnpj_data(cnpj)
                fields = parse_api_fields(data) if data else {}
                if not fields.get("telefone") or not fields.get("email"):
                    time.sleep(1)
                    s_phone, s_email = scrape_fallback(fields.get("razao_social"))
                    fields["telefone"] = fields.get("telefone") or s_phone
                    fields["email"] = fields.get("email") or s_email
                time.sleep(1)
                block = format_output_block(cnpj, fields)
                out.write(block + "\n\n")
                out.flush()
            except Exception as exc:
                out.write(f"ERROR processing CNPJ: {raw}\n{exc}\n\n")
                out.flush()
            finally:
                processed += 1
                state = {"status": "running", "processed": processed, "total": total, "file": out_path}
                _write_json(status_path, state)

    state = {"status": "done", "processed": processed, "total": total, "file": out_path}
    _write_json(status_path, state)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/process", methods=["POST"])
def process():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if not file.filename.lower().endswith(".txt"):
        return jsonify({"error": "Only .txt files are allowed"}), 400
    content = file.stream.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in content.splitlines() if line.strip()]

    task_id = uuid.uuid4().hex
    # Start background processing
    with TASK_LOCK:
        TASKS[task_id] = {"status": "queued", "processed": 0, "total": len(lines)}
    t = Thread(target=_background_process, args=(task_id, lines), daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/status/<task_id>")
def status(task_id: str):
    status_path = os.path.join(OUTPUT_DIR, f"{task_id}.json")
    data = _read_json(status_path) or {"status": "unknown"}
    return jsonify(data)


@app.route("/download")
def download():
    # Retrieve last result file path and stream it; fall back to in-memory
    file_path = session.get("result_path")
    if file_path and os.path.isfile(file_path):
        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path), mimetype="text/plain; charset=utf-8")
    sid = session.get("sid")
    result_bytes = RESULT_STORE.get(sid, b"")
    mem = io.BytesIO(result_bytes)
    mem.seek(0)
    filename = f"resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    return send_file(mem, as_attachment=True, download_name=filename, mimetype="text/plain; charset=utf-8")


@app.route("/download/<task_id>")
def download_task(task_id: str):
    status_path = os.path.join(OUTPUT_DIR, f"{task_id}.json")
    data = _read_json(status_path)
    if not data or "file" not in data:
        return jsonify({"error": "Task not found"}), 404
    file_path = data.get("file")
    if not (file_path and os.path.isfile(file_path)):
        return jsonify({"error": "Result not ready"}), 400
    return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path), mimetype="text/plain; charset=utf-8")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)


