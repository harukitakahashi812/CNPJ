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
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import os as _os
import csv
import os


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

# simple in-memory result store per session id
RESULT_STORE: Dict[str, bytes] = {}

# background task bookkeeping (also mirrored to disk for multi-worker safety)
TASK_LOCK = Lock()
TASKS: Dict[str, Dict] = {}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# small in-memory contact cache to avoid repeated scrapes per run
CONTACT_CACHE: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
BANK_MAP_PATHS = [
    os.path.join(os.path.dirname(__file__), "bank.csv"),
    os.path.join(os.path.dirname(__file__), "bank_map.csv"),
    os.path.join(os.path.dirname(__file__), "data", "bank.csv"),
]


def load_bank_map() -> Dict[str, Dict[str, str]]:
    bank_map: Dict[str, Dict[str, str]] = {}
    for p in BANK_MAP_PATHS:
        try:
            if os.path.isfile(p):
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        c = clean_cnpj(row.get("cnpj", ""))
                        if not c:
                            continue
                        bank_map[c] = {
                            "agencia": (row.get("agencia", "") or "").strip(),
                            "conta": (row.get("conta", "") or "").strip(),
                        }
                print(f"Loaded bank data from {p}: {len(bank_map)} entries")
                break
        except Exception as e:
            print(f"Failed to load bank data from {p}: {e}")
            continue
    return bank_map


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
]


def rotate_headers() -> Dict[str, str]:
    agent = USER_AGENTS[int(time.time()) % len(USER_AGENTS)]
    return {"User-Agent": agent}


class RateLimiter:
    def __init__(self, min_interval_seconds: float = 0.5):  # Reduced from 1.0 to 0.5
        self.min_interval_seconds = min_interval_seconds
        self._last_by_host: Dict[str, float] = {}
        self._lock = Lock()

    def wait(self, host: str) -> None:
        with self._lock:
            now = time.time()
            last = self._last_by_host.get(host, 0.0)
            to_wait = self.min_interval_seconds - (now - last)
            if to_wait > 0:
                time.sleep(to_wait)
                now = time.time()
            self._last_by_host[host] = now


RATE_LIMITER = RateLimiter(0.5)  # Reduced from 1.0 to 0.5


def rate_limited_get(url: str, headers: Dict[str, str], timeout: int) -> requests.Response:
    host = urlparse(url).netloc
    RATE_LIMITER.wait(host)
    return requests.get(url, headers=headers, timeout=timeout)


def get_json_with_retries(url: str, headers: Dict[str, str], timeout: int = 30, attempts: int = 3) -> Optional[dict]:
    for i in range(attempts):
        try:
            resp = rate_limited_get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass
        time.sleep(0.5 + i * 0.5)  # Reduced sleep time
    return None


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
    # Primary: publica.cnpj.ws
    primary = get_json_with_retries(f"https://publica.cnpj.ws/cnpj/{cnpj}", headers={"User-Agent": "Mozilla/5.0"})
    if primary:
        primary["_source"] = "publica"
        return primary
    # Fallback: BrasilAPI
    fallback = get_json_with_retries(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", headers=rotate_headers())
    if fallback:
        # Normalize to close shape expected by parse_api_fields
        normalized = {
            "razao_social": fallback.get("razao_social") or fallback.get("nome_fantasia") or fallback.get("nome"),
            "natureza_juridica": {"descricao": (fallback.get("natureza_juridica") or "")},
            "descricao_situacao_cadastral": fallback.get("situacao_cadastral") or fallback.get("situacao") or "",
            "data_inicio_atividade": fallback.get("data_inicio_atividade") or fallback.get("data_situacao_cadastral") or fallback.get("data_inicio_atividades") or "",
            "porte": {"descricao": fallback.get("porte") or ""},
            "opcao_pelo_mei": bool(fallback.get("mei") or (fallback.get("qualificacao_do_responsavel") == "MEI")),
            "estabelecimento": {
                "telefone1": (fallback.get("ddd_telefone_1") or fallback.get("telefone") or "").replace(" ", ""),
                "telefone2": fallback.get("ddd_telefone_2") or "",
                "email": fallback.get("email") or "",
            },
            "_source": "brasilapi",
        }
        return normalized
    return None


def parse_api_fields(data: dict) -> Dict[str, Optional[str]]:
    if not data:
        return {}
    estabelecimento = data.get("estabelecimento") or {}
    natureza = data.get("natureza_juridica") or {}
    porte = data.get("porte") or {}

    # Try multiple phone fields
    telefone = (
        estabelecimento.get("telefone1") or 
        estabelecimento.get("telefone2") or 
        estabelecimento.get("telefone") or
        data.get("telefone") or
        data.get("ddd_telefone_1") or
        data.get("ddd_telefone_2")
    )
    
    # Try multiple email fields
    email = (
        estabelecimento.get("email") or
        data.get("email") or
        data.get("email_empresa") or
        data.get("email_contato")
    )

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


def _extract_phone_email(text: str) -> Tuple[Optional[str], Optional[str]]:
    phone = None
    email = None
    
    # Improved phone regex patterns
    phone_patterns = [
        r"(?:\(?(\d{2})\)?\s*)?(\d{4,5})[-\s]?(\d{4})",  # (11) 99999-9999 or 11999999999
        r"(\d{2})\s*(\d{4,5})\s*(\d{4})",  # 11 99999 9999
        r"(\d{10,11})",  # 11999999999
    ]
    
    for pattern in phone_patterns:
        pm = re.search(pattern, text)
        if pm:
            if len(pm.groups()) == 3:
                phone = "".join(pm.groups())
            elif len(pm.groups()) == 1:
                phone = pm.group(1)
            if phone and len(phone) >= 10:
                break
    
    # Improved email regex patterns
    email_patterns = [
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",  # Standard email
        r"[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Za-z]{2,}",  # Email with spaces
        r"[A-Za-z0-9._%+-]+\s*\[at\]\s*[A-Za-z0-9.-]+\s*\[dot\]\s*[A-Za-z]{2,}",  # [at] [dot] format
    ]
    
    for pattern in email_patterns:
        em = re.search(pattern, text)
        if em:
            email = em.group(0).replace(" ", "").replace("[at]", "@").replace("[dot]", ".")
            if "@" in email and "." in email.split("@")[1]:
                break
    
    return phone, email


def scrape_telelistas(query: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        url = f"https://www.telelistas.net/busca?q={requests.utils.quote(query)}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if r.status_code != 200:
            return None, None
        phone, email = _extract_phone_email(r.text)
        return phone, email
    except Exception:
        return None, None


def scrape_consultasocio(query: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        # First try search by company name
        search_url = f"https://www.consultasocio.com/q/{requests.utils.quote(query)}"
        r = rate_limited_get(search_url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")
        company_link = soup.select_one('a[href*="/empresa/"]')
        if company_link and company_link.get('href'):
            detail_url = "https://www.consultasocio.com" + company_link.get('href')
            d = rate_limited_get(detail_url, headers=rotate_headers(), timeout=15)  # Reduced timeout
            if d.status_code == 200:
                text = BeautifulSoup(d.text, "html.parser").get_text(" ")
                phone, email = _extract_phone_email(text)
                return phone, email
        return None, None
    except Exception:
        return None, None


def scrape_cnpj_biz(cnpj: Optional[str], company_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if cnpj:
            url = f"https://cnpj.biz/{cnpj}"
            r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
            if r.status_code == 200:
                return _extract_phone_email(r.text)
        if company_name:
            url = f"https://cnpj.biz/search?q={requests.utils.quote(company_name)}"
            r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
            if r.status_code == 200:
                return _extract_phone_email(r.text)
    except Exception:
        return None, None
    return None, None


def scrape_guiamais(company_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not company_name:
            return None, None
        slug = requests.utils.quote(company_name)
        url = f"https://www.guiamais.com.br/busca/{slug}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if r.status_code != 200:
            return None, None
        return _extract_phone_email(r.text)
    except Exception:
        return None, None


def scrape_cnpj_info(cnpj: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not cnpj:
            return None, None
        url = f"https://cnpj.info/{cnpj}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if r.status_code == 200:
            return _extract_phone_email(r.text)
    except Exception:
        return None, None
    return None, None


def scrape_empresite(query: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not query:
            return None, None
        # Use Jusbrasil Empresite search
        url = f"https://www.jusbrasil.com.br/empresas/busca?q={requests.utils.quote(query)}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if r.status_code == 200:
            return _extract_phone_email(r.text)
    except Exception:
        return None, None


def scrape_empresascnpj(query: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not query:
            return None, None
        url = f"https://empresascnpj.com/busca?q={requests.utils.quote(query)}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)
        if r.status_code == 200:
            return _extract_phone_email(r.text)
    except Exception:
        return None, None


def scrape_cnpjro(query: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not query:
            return None, None
        url = f"https://cnpj.ro/busca?q={requests.utils.quote(query)}"
        r = rate_limited_get(url, headers=rotate_headers(), timeout=15)
        if r.status_code == 200:
            return _extract_phone_email(r.text)
    except Exception:
        return None, None


def scrape_google_cse(query: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        cse_id = os.environ.get("GOOGLE_CSE_ID")
        if not query or not api_key or not cse_id:
            return None, None
        api_url = (
            "https://www.googleapis.com/customsearch/v1?key="
            + requests.utils.quote(api_key)
            + "&cx="
            + requests.utils.quote(cse_id)
            + "&num=3&q="
            + requests.utils.quote(query)
        )
        sres = rate_limited_get(api_url, headers=rotate_headers(), timeout=15)  # Reduced timeout
        if sres.status_code != 200:
            return None, None
        js = sres.json()
        items = js.get("items") or []
        for it in items:
            link = it.get("link")
            if not link:
                continue
            try:
                page = rate_limited_get(link, headers=rotate_headers(), timeout=15)  # Reduced timeout
                if page.status_code == 200:
                    phone, email = _extract_phone_email(page.text)
                    if phone or email:
                        return phone, email
            except Exception:
                continue
        return None, None
    except Exception:
        return None, None


def scrape_fallback(company_name: Optional[str], cnpj: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    # Return cached value first
    cache_key = (cnpj or "") + "|" + (company_name or "")
    if cache_key in CONTACT_CACHE:
        return CONTACT_CACHE[cache_key]

    # Run multiple lightweight scrapers in parallel and return first hit
    sources = []
    if company_name:
        sources.append((scrape_telelistas, (company_name,)))
        sources.append((scrape_consultasocio, (company_name,)))
        sources.append((scrape_guiamais, (company_name,)))
        sources.append((scrape_empresite, (company_name,)))
        sources.append((scrape_empresascnpj, (company_name,)))
        sources.append((scrape_cnpjro, (company_name,)))
        sources.append((scrape_google_cse, (company_name,)))
    sources.append((scrape_cnpj_biz, (cnpj, company_name)))
    sources.append((scrape_cnpj_info, (cnpj,)))

    with ThreadPoolExecutor(max_workers=len(sources)) as ex:
        futures = [ex.submit(fn, *args) for fn, args in sources]
        for f in as_completed(futures):
            try:
                p, e = f.result()
                if p or e:
                    CONTACT_CACHE[cache_key] = (p, e)
                    return p, e
            except Exception:
                continue
    CONTACT_CACHE[cache_key] = (None, None)
    return None, None


def format_output_block(cnpj: str, fields: Dict[str, Optional[str]], agencia: str = "", conta: str = "") -> str:
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
        f"AGENCIA: {agencia}",
        f"CONTA: {conta}",
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
            time.sleep(0.5)  # Reduced from 1.0 to 0.5
            s_phone, s_email = scrape_fallback(fields.get("razao_social"))
            fields["telefone"] = fields.get("telefone") or s_phone
            fields["email"] = fields.get("email") or s_email

        # Sleep to avoid rate limits
        time.sleep(0.5)  # Reduced from 1.0 to 0.5

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


def _process_single_cnpj(cnpj_raw: str, bank_map: Dict[str, Dict[str, str]]) -> str:
    cnpj = clean_cnpj(cnpj_raw)
    if not cnpj:
        return ""
    
    # Fetch API data
    data = fetch_cnpj_data(cnpj)
    fields = parse_api_fields(data) if data else {}
    
    # If phone or email missing, scrape
    if not fields.get("telefone") or not fields.get("email"):
        s_phone, s_email = scrape_fallback(fields.get("razao_social"), cnpj)
        fields["telefone"] = fields.get("telefone") or s_phone
        fields["email"] = fields.get("email") or s_email
    
    # Get bank data
    bank = bank_map.get(cnpj) or {}
    
    return format_output_block(cnpj, fields, agencia=bank.get("agencia", ""), conta=bank.get("conta", ""))


def _background_process(task_id: str, cnpjs: List[str], bank_map: Optional[Dict[str, Dict[str, str]]] = None) -> None:
    status_path = os.path.join(OUTPUT_DIR, f"{task_id}.json")
    out_path = os.path.join(OUTPUT_DIR, f"resultado_{task_id}.txt")
    
    with open(out_path, "w", encoding="utf-8") as out:
        total = len(cnpjs)
        processed = 0
        state = {"status": "running", "processed": 0, "total": total, "file": out_path}
        _write_json(status_path, state)

        bank_map = bank_map or {}

        # Parallel processing with optimized workers
        # Allow tuning via env; default 16 (increased from 12)
        try:
            env_workers = int(os.environ.get("PARALLEL_WORKERS", "16"))
        except Exception:
            env_workers = 16
        max_workers = max(1, env_workers)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_raw = {executor.submit(_process_single_cnpj, raw, bank_map): raw for raw in cnpjs}
            for future in as_completed(future_to_raw):
                raw = future_to_raw[future]
                try:
                    block = future.result()
                    if block:
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

    # Load optional bank mapping from disk (bank.csv) to avoid requiring a second upload
    bank_map = load_bank_map()
    print(f"Loaded bank map with {len(bank_map)} entries")

    task_id = uuid.uuid4().hex
    # Start background processing
    with TASK_LOCK:
        TASKS[task_id] = {"status": "queued", "processed": 0, "total": len(lines)}
    t = Thread(target=_background_process, args=(task_id, lines, bank_map), daemon=True)
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


