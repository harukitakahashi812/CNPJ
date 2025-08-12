document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('uploadForm');
  const fileInput = document.getElementById('fileInput');
  const processBtn = document.getElementById('processBtn');
  const spinner = document.getElementById('spinner');
  const statusText = document.getElementById('statusText');
  const downloadBtn = document.getElementById('downloadBtn');

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
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed');
      statusText.textContent = 'Done.';
      downloadBtn.classList.remove('hidden');
    } catch (err) {
      statusText.textContent = 'Error.';
      alert(err.message || 'Failed to process');
    } finally {
      spinner.classList.add('hidden');
      processBtn.disabled = false;
    }
  });

  downloadBtn.addEventListener('click', () => {
    window.location.href = '/download';
  });
});


