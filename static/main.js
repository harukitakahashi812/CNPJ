document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('uploadForm');
  const fileInput = document.getElementById('fileInput');
  const processBtn = document.getElementById('processBtn');
  const spinner = document.getElementById('spinner');
  const statusText = document.getElementById('statusText');
  const downloadBtn = document.getElementById('downloadBtn');
  let currentTaskId = null;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!fileInput.files[0]) {
      alert('Please select a .txt file.');
      return;
    }
    const extOK = fileInput.files[0].name.toLowerCase().endsWith('.txt');
    if (!extOK) {
      alert('Only .txt files are allowed');
      return;
    }
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    processBtn.disabled = true;
    spinner.classList.remove('hidden');
    statusText.textContent = 'Processing...';
    downloadBtn.classList.add('hidden');

    try {
      const res = await fetch('/process', { method: 'POST', body: formData });
      const text = await res.text();
      let data; try { data = JSON.parse(text); } catch (e) { throw new Error('Server returned HTML instead of JSON. Please retry.'); }
      if (!res.ok) throw new Error(data.error || 'Failed');
      currentTaskId = data.task_id;
      statusText.textContent = 'Queued...';
      // poll status
      const poll = async () => {
        if (!currentTaskId) return;
        const r = await fetch(`/status/${currentTaskId}`);
        const s = await r.json();
        if (s && s.status === 'done') {
          statusText.textContent = `Done (${s.processed}/${s.total}).`;
          downloadBtn.classList.remove('hidden');
          return;
        }
        if (s && s.total) {
          statusText.textContent = `Processing ${s.processed}/${s.total}...`;
        }
        setTimeout(poll, 1500);
      };
      poll();
    } catch (err) {
      statusText.textContent = 'Error.';
      alert(err.message || 'Failed to process');
    } finally {
      spinner.classList.add('hidden');
      processBtn.disabled = false;
    }
  });

  downloadBtn.addEventListener('click', () => {
    if (currentTaskId) {
      window.location.href = `/download/${currentTaskId}`;
    } else {
      window.location.href = '/download';
    }
  });
});


