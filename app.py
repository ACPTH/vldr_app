"""
VLDR Generator - Flask Backend
Run: python app.py  |  Open: http://localhost:5050
Place PDF templates in ./templates/ folder (same directory as app.py)
"""
import os, io, zipfile, json, base64, time, threading, hashlib, tempfile, sqlite3, uuid, traceback, subprocess, shutil
from collections import deque, OrderedDict
from datetime import timedelta
from flask import Flask, request, jsonify, send_file, session, redirect
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, create_string_object, DictionaryObject
import pandas as pd

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
# SECRET_KEY must be set in environment for production (Render sets it automatically)
_default_key = 'vldr-local-dev-' + __import__('hashlib').md5(b'vldr').hexdigest()
app.secret_key = os.environ.get('SECRET_KEY', _default_key)
app.permanent_session_lifetime = timedelta(hours=8)

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
STATIC_DIR   = os.path.join(BASE_DIR, 'static')
DB_PATH_ENV  = os.environ.get('VLDR_DB_PATH', os.path.join(BASE_DIR, 'users.db'))
JOB_DIR      = os.environ.get('VLDR_JOB_DIR', os.path.join(os.path.dirname(DB_PATH_ENV), 'jobs'))

# Concurrency control (Render free is tight)
MAX_JOBS = int(os.environ.get('VLDR_MAX_JOBS', '2'))
QUEUE_TIMEOUT = float(os.environ.get('VLDR_QUEUE_TIMEOUT', '20'))  # seconds to wait in queue
_job_sem = threading.BoundedSemaphore(MAX_JOBS)
_job_lock = threading.Lock()
_active_jobs = 0
_queued_jobs = 0
_recent_times = deque(maxlen=30)
_default_cache_max = '20' if os.environ.get('RENDER') else '200'
MAX_CACHE = int(os.environ.get('VLDR_CACHE_MAX', _default_cache_max))
_pdf_cache = OrderedDict()  # key -> bytes
SPOOL_MAX_MB = int(os.environ.get('VLDR_SPOOL_MB', '16'))
JOB_POLL_SEC = float(os.environ.get('VLDR_JOB_POLL_SEC', '1.2'))
JOB_BATCH_SIZE = int(os.environ.get('VLDR_JOB_BATCH_SIZE', '5'))
PDF_COMPRESS_ENABLED = os.environ.get('VLDR_PDF_COMPRESS', '1') != '0'
PDF_COMPRESS_MODE = (os.environ.get('VLDR_PDF_COMPRESS_MODE', 'lossless') or 'lossless').strip().lower()
PDF_TARGET_MAX_KB = int(os.environ.get('VLDR_PDF_TARGET_MAX_KB', '400'))
PDF_TARGET_MAX_BYTES = max(1, PDF_TARGET_MAX_KB) * 1024

@app.errorhandler(Exception)
def handle_any_exception(e):
    """Ensure API endpoints always return JSON, never HTML error pages."""
    if request.path.startswith('/api/'):
        if isinstance(e, HTTPException):
            return jsonify({'error': e.description or str(e)}), e.code or 500
        return jsonify({'error': str(e)}), 500
    if isinstance(e, HTTPException):
        return e
    return 'Internal Server Error', 500

def _start_job_or_queue():
    """Acquire a job slot. Wait up to QUEUE_TIMEOUT. Returns (ok, waited_sec)."""
    global _active_jobs, _queued_jobs
    start_wait = time.time()
    with _job_lock:
        _queued_jobs += 1
    acquired = _job_sem.acquire(timeout=QUEUE_TIMEOUT)
    waited = time.time() - start_wait
    with _job_lock:
        _queued_jobs = max(0, _queued_jobs - 1)
        if acquired:
            _active_jobs += 1
    return acquired, waited

def _end_job(start_ts):
    global _active_jobs
    with _job_lock:
        _active_jobs = max(0, _active_jobs - 1)
    _job_sem.release()
    _recent_times.append(time.time() - start_ts)

def _job_stats():
    with _job_lock:
        active = _active_jobs
        queued = _queued_jobs
    avg = sum(_recent_times) / len(_recent_times) if _recent_times else 0
    return {'active': active, 'queued': queued, 'max': MAX_JOBS, 'avg_sec': round(avg, 2)}

def _is_empty_damage(val):
    t = str(val or '').strip().lower()
    return t in ('', '-', 'nan', 'none')

def filter_damage_records(records):
    """Drop lines with no real damage info."""
    out = []
    for r in records or []:
        if not r: continue
        p = r.get('damage_part_code', '')
        t = r.get('damage_type_code', '')
        e = r.get('damage_extent', '') or r.get('damage_extent_code', '')
        c = r.get('damage_classification', '')
        if _is_empty_damage(p) and _is_empty_damage(t) and _is_empty_damage(e) and _is_empty_damage(c):
            continue
        out.append(r)
    return out

def _cache_get(key):
    if key in _pdf_cache:
        _pdf_cache.move_to_end(key)
        return _pdf_cache[key]
    return None

def _cache_set(key, data_bytes):
    _pdf_cache[key] = data_bytes
    _pdf_cache.move_to_end(key)
    while len(_pdf_cache) > MAX_CACHE:
        _pdf_cache.popitem(last=False)

def _hash_records(records):
    return hashlib.md5(json.dumps(records, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()

def get_cached_flat(fmt, vin, records, manual):
    """Return BytesIO of flattened PDF, cached per VIN+fmt+manual+records."""
    key = (fmt, vin, _hash_records(records), json.dumps(manual or {}, sort_keys=True, ensure_ascii=False))
    cached = _cache_get(key)
    if cached:
        return io.BytesIO(cached)
    tpl = get_template(fmt)
    fsz = FONT_SIZES.get(fmt, {})
    comb_skip = comb_skip_for(fmt)
    flat = fill_pdf_and_overlay_comb(tpl, BUILDERS[fmt](vin, records, manual), fsz, comb_skip=comb_skip)
    data = flat.read()
    _cache_set(key, data)
    return io.BytesIO(data)

def get_uncached_flat(fmt, vin, records, manual):
    """Return flattened PDF bytes without touching LRU cache (lower memory for big batches)."""
    tpl = get_template(fmt)
    fsz = FONT_SIZES.get(fmt, {})
    comb_skip = comb_skip_for(fmt)
    return fill_pdf_and_overlay_comb(tpl, BUILDERS[fmt](vin, records, manual), fsz, comb_skip=comb_skip)

def _spooled_buffer():
    """In-memory buffer that spills to disk after threshold."""
    return tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MB * 1024 * 1024, mode='w+b')

def _read_all_bytes(buf):
    try:
        buf.seek(0)
        return buf.read()
    except Exception:
        return b''

def _find_gs_binary():
    # Windows common names + unix
    for cmd in ('gs', 'gswin64c', 'gswin32c'):
        p = shutil.which(cmd)
        if p:
            return p
    return None

def _ghostscript_compress(raw_pdf_bytes):
    """
    Lossy compression with Ghostscript for aggressive size reduction.
    Falls back to original on any issue.
    """
    gs_bin = _find_gs_binary()
    if not gs_bin:
        return raw_pdf_bytes
    preset = '/ebook' if PDF_COMPRESS_MODE == 'balanced' else '/screen'
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as in_f:
        in_f.write(raw_pdf_bytes)
        in_path = in_f.name
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as out_f:
        out_path = out_f.name
    try:
        cmd = [
            gs_bin,
            '-sDEVICE=pdfwrite',
            '-dCompatibilityLevel=1.4',
            f'-dPDFSETTINGS={preset}',
            '-dNOPAUSE',
            '-dQUIET',
            '-dBATCH',
            '-dDetectDuplicateImages=true',
            '-dCompressFonts=true',
            '-dSubsetFonts=true',
            '-dDownsampleColorImages=true',
            '-dColorImageDownsampleType=/Bicubic',
            '-dColorImageResolution=150' if PDF_COMPRESS_MODE == 'balanced' else '-dColorImageResolution=110',
            '-dDownsampleGrayImages=true',
            '-dGrayImageDownsampleType=/Bicubic',
            '-dGrayImageResolution=150' if PDF_COMPRESS_MODE == 'balanced' else '-dGrayImageResolution=110',
            '-dDownsampleMonoImages=true',
            '-dMonoImageResolution=300' if PDF_COMPRESS_MODE == 'balanced' else '-dMonoImageResolution=220',
            f'-sOutputFile={out_path}',
            in_path
        ]
        proc = subprocess.run(cmd, capture_output=True, check=False)
        if proc.returncode != 0 or (not os.path.exists(out_path)):
            return raw_pdf_bytes
        with open(out_path, 'rb') as fh:
            out = fh.read()
        if not out:
            return raw_pdf_bytes
        return out
    except Exception:
        return raw_pdf_bytes
    finally:
        try:
            if os.path.exists(in_path):
                os.remove(in_path)
        except Exception:
            pass
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass

def compress_pdf_lossless(pdf_buf):
    """Conservative compression with safe fallback.
    - lossless: pypdf rewrite/compress only
    - balanced/strong: Ghostscript only when above size target
    """
    raw = _read_all_bytes(pdf_buf)
    if not raw:
        return io.BytesIO()
    if not PDF_COMPRESS_ENABLED:
        return io.BytesIO(raw)
    mode = PDF_COMPRESS_MODE if PDF_COMPRESS_MODE in ('lossless', 'balanced', 'strong') else 'lossless'
    if mode in ('balanced', 'strong'):
        # Speed optimization: skip expensive GS pass for already-small files.
        if len(raw) <= PDF_TARGET_MAX_BYTES:
            return io.BytesIO(raw)
        gs_out = _ghostscript_compress(raw)
        if gs_out and len(gs_out) < len(raw):
            return io.BytesIO(gs_out)
        return io.BytesIO(raw)
    try:
        reader = PdfReader(io.BytesIO(raw))
        writer = PdfWriter()
        writer.append(reader)
        for page in writer.pages:
            if hasattr(page, 'compress_content_streams'):
                try:
                    page.compress_content_streams()
                except Exception:
                    pass
        out = io.BytesIO()
        writer.write(out)
        out.seek(0)
        compressed = out.getvalue()
        if not compressed or len(compressed) > len(raw):
            best = raw
        else:
            best = compressed
        return io.BytesIO(best)
    except Exception:
        return io.BytesIO(raw)

def _jobs_connect():
    con = sqlite3.connect(os.path.join(os.path.dirname(DB_PATH_ENV), 'jobs.db'))
    con.row_factory = sqlite3.Row
    return con

def init_jobs_db():
    os.makedirs(JOB_DIR, exist_ok=True)
    con = _jobs_connect()
    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            message TEXT,
            error TEXT,
            payload TEXT NOT NULL,
            result_path TEXT,
            result_name TEXT,
            total_items INTEGER NOT NULL DEFAULT 0,
            done_items INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)')
    con.commit()
    cur.close()
    con.close()

def create_job(payload_dict):
    job_id = str(uuid.uuid4())
    now = time.time()
    total = len(payload_dict.get('items', []) or [])
    con = _jobs_connect()
    cur = con.cursor()
    cur.execute('''
        INSERT INTO jobs (id, status, progress, message, payload, total_items, done_items, created_at, updated_at)
        VALUES (?, 'queued', 0, ?, ?, ?, 0, ?, ?)
    ''', (job_id, 'Queued', json.dumps(payload_dict, ensure_ascii=False), total, now, now))
    con.commit()
    cur.close()
    con.close()
    return job_id

def get_job(job_id):
    con = _jobs_connect()
    cur = con.cursor()
    cur.execute('SELECT * FROM jobs WHERE id=?', (job_id,))
    row = cur.fetchone()
    cur.close()
    con.close()
    return row

def update_job(job_id, **fields):
    if not fields:
        return
    fields['updated_at'] = time.time()
    sets = ', '.join([f'{k}=?' for k in fields.keys()])
    vals = list(fields.values()) + [job_id]
    con = _jobs_connect()
    cur = con.cursor()
    cur.execute(f'UPDATE jobs SET {sets} WHERE id=?', vals)
    con.commit()
    cur.close()
    con.close()

def claim_next_job():
    con = _jobs_connect()
    cur = con.cursor()
    try:
        cur.execute('BEGIN IMMEDIATE')
        cur.execute('SELECT id FROM jobs WHERE status=? ORDER BY created_at ASC LIMIT 1', ('queued',))
        row = cur.fetchone()
        if not row:
            con.commit()
            return None
        job_id = row['id']
        now = time.time()
        cur.execute('''
            UPDATE jobs
            SET status='running', message=?, progress=5, updated_at=?
            WHERE id=?
        ''', ('Starting...', now, job_id))
        con.commit()
        return job_id
    finally:
        cur.close()
        con.close()

def _chunks(lst, n):
    n = max(1, int(n))
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _safe_vin(v):
    return (v or '').replace('/', '_').replace('\\', '_')

def _is_fmt_allowed_for_payload(fmt, allowed_formats):
    if not allowed_formats:
        return True
    return fmt in allowed_formats

def process_job(job_id):
    row = get_job(job_id)
    if not row:
        return
    try:
        payload = json.loads(row['payload'] or '{}')
        items = payload.get('items', []) or []
        mode = payload.get('mode', 'individual')
        allowed_formats = payload.get('allowed_formats', None)
        recs = filter_damage_records(payload.get('records', []) or [])
        vg = group_by_vin(recs)
        total = len(items)
        update_job(job_id, message='Preparing...', total_items=total, done_items=0, progress=8)

        out_name = 'VLDR_selected_' + str(total) + 'vins.zip' if mode == 'individual' else 'VLDR_all_formats.zip'
        out_path = os.path.join(JOB_DIR, job_id + '.zip')
        written = 0

        with zipfile.ZipFile(out_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            if mode == 'individual':
                done = 0
                for batch in _chunks(items, JOB_BATCH_SIZE):
                    for item in batch:
                        fmt = (item.get('format', '') or '').upper()
                        vin = item.get('vin', '') or ''
                        manual = item.get('manual', {}) or {}
                        done += 1
                        if fmt not in BUILDERS or vin not in vg or not _is_fmt_allowed_for_payload(fmt, allowed_formats):
                            pct = 10 + int((done / max(1, total)) * 80)
                            update_job(job_id, done_items=done, progress=min(92, pct), message=f'Processing {done}/{total}')
                            continue
                        try:
                            flat = get_uncached_flat(fmt, vin, vg[vin], manual)
                            flat = compress_pdf_lossless(flat)
                            zf.writestr(_safe_vin(vin) + '.pdf', flat.read())
                            written += 1
                        except Exception:
                            pass
                        pct = 10 + int((done / max(1, total)) * 80)
                        update_job(job_id, done_items=done, progress=min(92, pct), message=f'Processing {done}/{total}')
            elif mode == 'all-merged':
                from collections import defaultdict
                by_fmt = defaultdict(list)
                manual_by_fmt = {}
                for item in items:
                    fmt = (item.get('format', '') or '').upper()
                    by_fmt[fmt].append(item.get('vin', '') or '')
                    manual_by_fmt[fmt] = item.get('manual', {}) or {}

                fmts = list(by_fmt.items())
                done = 0
                total_fmt = len(fmts) or 1
                for fmt, vins in fmts:
                    done += 1
                    if fmt not in BUILDERS or not _is_fmt_allowed_for_payload(fmt, allowed_formats):
                        pct = 10 + int((done / total_fmt) * 80)
                        update_job(job_id, progress=min(92, pct), message=f'Merging formats {done}/{total_fmt}')
                        continue
                    try:
                        merged = PdfWriter()
                        for vin in vins:
                            if vin not in vg:
                                continue
                            flat = get_uncached_flat(fmt, vin, vg[vin], manual_by_fmt.get(fmt, {}))
                            merged.append(PdfReader(flat))
                        pdf_out = _spooled_buffer()
                        merged.write(pdf_out)
                        pdf_out = compress_pdf_lossless(pdf_out)
                        zf.writestr('VLDR_' + fmt + '_' + str(len(vins)) + 'vins.pdf', pdf_out.read())
                        written += 1
                    except Exception:
                        pass
                    pct = 10 + int((done / total_fmt) * 80)
                    update_job(job_id, progress=min(92, pct), message=f'Merging formats {done}/{total_fmt}')
            else:
                raise ValueError('Unknown mode')

        if written <= 0:
            raise ValueError('No files generated')
        update_job(job_id, status='done', progress=100, message='Done', result_path=out_path, result_name=out_name)
    except Exception as e:
        update_job(job_id, status='error', progress=100, message='Error', error=str(e) + '\n' + traceback.format_exc())

def job_worker_loop():
    while True:
        job_id = claim_next_job()
        if not job_id:
            time.sleep(JOB_POLL_SEC)
            continue
        process_job(job_id)

_jobs_bootstrapped = False
_jobs_available = False
def bootstrap_jobs():
    global _jobs_bootstrapped, _jobs_available
    if _jobs_bootstrapped:
        return
    try:
        init_jobs_db()
        threading.Thread(target=job_worker_loop, daemon=True).start()
        _jobs_available = True
        _jobs_bootstrapped = True
    except Exception as e:
        # Never block app startup if async queue fails in constrained environments.
        _jobs_available = False
        _jobs_bootstrapped = True
        print('WARNING: async jobs disabled:', str(e))

# Auth - import after BASE_DIR is defined
from auth import init_db, do_login, do_logout, current_user
from auth import login_required, admin_required, get_all_users, create_user, update_user, delete_user
from auth import get_brand_format_map, save_brand_format_map

TEMPLATE_FILES = {
    'BMW':        'BMW_VLDR.PDF',
    'ECG':        'Damage_Report_Format.pdf',
    'FCA':        'SCHEDA_VLDR.PDF',
    'FORD':       'VLDR_FORD.PDF',
    'LINKCO':     'VLDR_LinkCo.pdf',
    'RENAULT':    'PV_Renault.pdf',
    'STELLANTIS': 'Constat_PSA.pdf',
    'VGED':       'VGED_VLDR.pdf',
    'VOLVO':      'VOLVO_VLDR.pdf',
}

FONT_SIZES = {
    'FCA': {**{'vin': 7}, **{f'Area{i}': 6 for i in range(1,8)},
            **{f'TD{i}': 6 for i in range(1,8)}, **{f'SD{i}': 6 for i in range(1,8)}},
    'BMW': {f: 6 for f in ['Parte','Defecto','Severidad','Origen',
                            'Parte2','Defecto2','Severidad2','Origen2',
                            'Parte3','Defecto3','Severidad3','Origen3',
                            'Parte4','Defecto4','Severidad4','Origen4']},
    'STELLANTIS': {'VIN': 7},
    'VOLVO': {'Date': 7},
}

STELLANTIS_TRANSPORT = {'t', 'transportation', 'damage (transport)'}

MANUAL_FIELDS = {
    'BMW':        [('Vessel',           'Vessel / Buque',                  'text')],
    'ECG':        [('Vessel',           'Transport ID / Vessel',           'text'),
                   ('Customer',         'Customer',                        'text')],
    'FCA':        [('Customer',         'Societa Cedente (Delivering Co.)', 'text'),
                   ('code',             'Codice Cedente (Provider Code)',   'text'),
                   ('Vessel',           'Targa / Plate / Vessel',          'text')],
    'FORD':       [('Carrier',          'Delivering Carrier',              'text'),
                   ('Vessel',           'Truck No. / Ship',                'text'),
                   ('Receptor',         'Receiving Carrier Name',          'text')],
    'LINKCO':     [('Customer',         'Inspector Name',                  'text'),
                   ('Vessel',           'Transport Ref / Vessel',          'text')],
    'RENAULT':    [('Destination',      'Destination',                     'text'),
                   ('Customer',         'Sending Party (Partie Cedante)',   'text'),
                   ('N Parte',          'N Document',                      'text')],
    'STELLANTIS': [('Vessel',           'Immat. Camion / Vessel',          'text'),
                   ('Refuse',           'N Doc. Transport',                'text')],
    'VGED':       [('Comp Responsable', 'Operador Logistico (Delivering)', 'text')],
    'VOLVO':      [('Customer',         'Company',                         'text'),
                   ('DeliveryPart',     'Transport Mode',                  'text'),
                   ('Vessel',           'Transport Ref / Vessel',          'text')],
}

#  Helpers 
def s(v):
    if v is None: return ''
    t = str(v).strip()
    return '' if t in ('nan','NaN','None','-') else t

def is_stellantis_transport(cls):
    return s(cls).lower().strip() in STELLANTIS_TRANSPORT

def orig_bmw(cls):
    c = s(cls).upper()
    if any(x in c for x in ['TRANSPORT','T-WPO','OTTD']): return 'T'
    if any(x in c for x in ['FACTOR','FABR']): return 'F'
    return 'I'

def comb_skip_for(fmt):
    f = (fmt or '').upper()
    if f == 'FCA': return {'vin'}
    if f == 'STELLANTIS': return {'VIN'}
    return set()

def allowed_formats_for_session():
    u = current_user()
    if not u.get('uid'): return None
    if u.get('role') == 'admin': return None
    return u.get('allowed_formats')  # None => all

def is_format_allowed(fmt):
    allowed = allowed_formats_for_session()
    if not allowed: return True
    return fmt in allowed

def get_template(fmt):
    fname = TEMPLATE_FILES.get(fmt, '')
    path  = os.path.join(TEMPLATE_DIR, fname)
    if os.path.exists(path):
        return path
    # Case-insensitive fallback (Windows sometimes saves with different case)
    try:
        files_in_dir = os.listdir(TEMPLATE_DIR)
    except Exception:
        files_in_dir = []
    for f in files_in_dir:
        if f.lower() == fname.lower():
            return os.path.join(TEMPLATE_DIR, f)
    raise FileNotFoundError(
        'Template not found: ' + fname +
        '\nExpected at: ' + path +
        '\nFiles in templates folder: ' + str(files_in_dir)
    )

def flatten_with_pdftk(pdf_buf):
    """Flatten PDF: bake field appearances into page content.
    Prefer real pdftk when available; fallback to pure Python.
    """
    import tempfile, subprocess
    # Try pdftk CLI first (more reliable for Acrobat)
    try:
        pdf_buf.seek(0)
        data = pdf_buf.read()
        fd_in, path_in = tempfile.mkstemp(suffix='.pdf')
        fd_out, path_out = tempfile.mkstemp(suffix='.pdf')
        os.close(fd_in); os.close(fd_out)
        with open(path_in, 'wb') as f:
            f.write(data)
        subprocess.run(['pdftk', path_in, 'output', path_out, 'flatten'],
                       capture_output=True, check=True)
        with open(path_out, 'rb') as f:
            out = io.BytesIO(f.read())
        out.seek(0)
        try:
            os.remove(path_in); os.remove(path_out)
        except Exception:
            pass
        return out
    except Exception:
        try:
            if 'path_in' in locals() and os.path.exists(path_in): os.remove(path_in)
            if 'path_out' in locals() and os.path.exists(path_out): os.remove(path_out)
        except Exception:
            pass

    # Fallback: pure Python flatten (best-effort)
    from pypdf.generic import DecodedStreamObject
    pdf_buf.seek(0)
    reader = PdfReader(pdf_buf)
    writer = PdfWriter()
    writer.append(reader)
    page = writer.pages[0]
    annots = page.get('/Annots', [])
    overlay_parts = []
    for ref in annots:
        try:
            obj = ref.get_object()
            ap = obj.get('/AP')
            if not ap: continue
            ap_obj = ap.get_object() if hasattr(ap, 'get_object') else ap
            n_ref = ap_obj.get('/N')
            if not n_ref: continue
            n_obj = n_ref.get_object() if hasattr(n_ref, 'get_object') else n_ref
            ap_bytes = n_obj.get_data()
            rect = obj.get('/Rect')
            if not ap_bytes or not rect: continue
            x0, y0 = float(rect[0]), float(rect[1])
            txt = ap_bytes.decode('latin-1', errors='replace').strip()
            overlay_parts.append('q 1 0 0 1 ' + str(round(x0,3)) + ' ' + str(round(y0,3)) + ' cm\n' + txt + '\nQ')
        except: continue
    if overlay_parts:
        from pypdf.generic import ArrayObject
        overlay = '\n'.join(overlay_parts).encode('latin-1', errors='replace')
        stream = DecodedStreamObject()
        stream.set_data(overlay)
        stream_ref = writer._add_object(stream)
        existing = page.get('/Contents')
        if existing is None:
            page[NameObject('/Contents')] = stream_ref
        elif isinstance(existing, ArrayObject):
            existing.append(stream_ref)
        else:
            page[NameObject('/Contents')] = ArrayObject([existing, stream_ref])
    if '/Annots' in page:
        del page[NameObject('/Annots')]
    if '/AcroForm' in writer._root_object:
        del writer._root_object[NameObject('/AcroForm')]
    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return out

def flatten_pdf(writer):
    """Legacy no-op."""
    pass

def _get_comb_field_coords(template_path):
    """Return {field_name: [x0,y0,x1,y1]} for fields with Comb flag (Ff & 0x1800000)."""
    coords = {}
    for page in PdfReader(template_path).pages:
        for ref in page.get('/Annots', []):
            obj = ref.get_object()
            name = obj.get('/T', '')
            if int(str(obj.get('/Ff', 0))) & 0x1800000:
                rect = obj.get('/Rect')
                if rect:
                    coords[name] = [float(x) for x in rect]
    return coords

def fill_pdf(template_path, field_data, font_sizes):
    """Fill PDF form fields.
    Key fix: delete ALL existing /AP streams before auto_regenerate=True.
    Fields with pre-existing /AP (e.g. Damage1-9 in Stellantis) keep their
    old empty appearance and ignore the new value unless /AP is cleared first.
    """
    from pypdf.generic import BooleanObject
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.append(reader)
    # Step 1: clear ALL existing /AP and /AS streams on every annotation
    # so auto_regenerate creates fresh appearances with the correct new values
    for page in writer.pages:
        for ref in page.get('/Annots', []):
            obj = ref.get_object()
            if '/AP' in obj:
                del obj[NameObject('/AP')]
            if '/AS' in obj:
                del obj[NameObject('/AS')]
    # Step 2: apply custom font sizes on fields BEFORE regenerating /AP
    # so the regenerated appearances use the updated /DA
    if font_sizes:
        annots = writer.pages[0].get('/Annots', [])
        for ref in annots:
            obj = ref.get_object()
            fn  = obj.get('/T', '')
            if fn in font_sizes:
                da = '/Helvetica ' + str(font_sizes[fn]) + ' Tf 0 g'
                if hasattr(ref, 'idnum'):
                    direct = writer._objects[ref.idnum - 1]
                    if isinstance(direct, DictionaryObject):
                        direct[NameObject('/DA')] = create_string_object(da)
                        if '/AP' in direct: del direct['/AP']
    # Step 3: fill fields - auto_regenerate=True creates /AP for all fields
    writer.update_page_form_field_values(writer.pages[0], field_data, auto_regenerate=True)
    # Step 4: set NeedAppearances so viewer regenerates any remaining fields
    if '/AcroForm' in writer._root_object:
        acro = writer._root_object['/AcroForm']
        if hasattr(acro, 'get_object'):
            acro = acro.get_object()
        acro[NameObject('/NeedAppearances')] = BooleanObject(True)
    buf = io.BytesIO()
    writer.write(buf)
    buf.seek(0)
    return buf

def fill_pdf_and_overlay_comb(template_path, field_data, font_sizes, comb_skip=None):
    """Fill PDF and add text overlay for comb fields that pdftk drops after flatten."""
    from pypdf.generic import DecodedStreamObject, ArrayObject as ArrObj
    comb_coords = _get_comb_field_coords(template_path)
    buf = fill_pdf(template_path, field_data, font_sizes)
    flat = flatten_with_pdftk(buf)

    # Add text overlay for any comb fields with values
    comb_skip = set(comb_skip or [])
    comb_values = {k: field_data.get(k, '') for k in comb_coords
                   if field_data.get(k) and k not in comb_skip}
    if not comb_values:
        return flat

    flat.seek(0)
    reader2 = PdfReader(flat)
    writer2 = PdfWriter()
    writer2.append(reader2)
    page2 = writer2.pages[0]

    lines = ['q', '0 0 0 rg']
    for name, value in comb_values.items():
        rect = comb_coords[name]
        x = rect[0] + 2
        y = rect[1] + 3
        h = rect[3] - rect[1]
        fs = max(6, min(9, h * 0.65))
        esc = str(value).replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
        lines.append(f'BT /Helvetica {fs:.1f} Tf {x:.2f} {y:.2f} Td ({esc}) Tj ET')
    lines.append('Q')

    stream = DecodedStreamObject()
    stream.set_data('\n'.join(lines).encode('latin-1'))
    ref = writer2._add_object(stream)

    existing = page2.get('/Contents')
    if isinstance(existing, ArrObj):
        page2[NameObject('/Contents')] = ArrObj(list(existing) + [ref])
    elif existing:
        page2[NameObject('/Contents')] = ArrObj([existing, ref])
    else:
        page2[NameObject('/Contents')] = ref

    out = io.BytesIO()
    writer2.write(out)
    out.seek(0)
    return out

def remark_join(records):
    return ' | '.join(s(r.get('damage_remark','')) for r in records if s(r.get('damage_remark','')))

#  Builders 
def build_BMW(vin, records, manual):
    f = records[0]
    dp = s(f.get('date','')).split('-')
    yr,mo,dy = (dp+['','',''])[:3]
    flds = {'Vin':s(vin),'Dia':dy,'Mes':mo,'Ano':yr,'A\u00f1o':yr,
            'Vessel':s(manual.get('Vessel','')),'Remark':remark_join(records)}
    for i,r in enumerate(records[:4]):
        sfx=['','2','3','4'][i]
        flds['Parte'+sfx]=s(r['damage_part_code'])
        flds['Defecto'+sfx]=s(r['damage_type_code'])
        flds['Severidad'+sfx]=s(r['damage_extent_code'])
        flds['Origen'+sfx]=orig_bmw(r.get('damage_classification',''))
    return flds

def build_ECG(vin, records, manual):
    f = records[0]
    flds = {'VIN':s(vin),'Make':s(f.get('make','')),'Model':s(f.get('model','')),
            'Date':s(f.get('date','')),'Location':s(f.get('location','')),
            'Vessel':s(manual.get('Vessel','')),'Customer':s(manual.get('Customer','')),
            'General_Remark':remark_join(records)}
    for i,r in enumerate(records[:5],1):
        flds['Part'+str(i)]=s(r['damage_part_code'])
        flds['Type'+str(i)]=s(r['damage_type_code'])
        flds['Extent'+str(i)]=s(r['damage_extent_code'])
        flds['Remark'+str(i)]=s(r.get('damage_classification',''))
    return flds

def build_FCA(vin, records, manual):
    vin8 = s(vin)[-8:] if len(s(vin))>=8 else s(vin)
    f = records[0]
    flds = {'vin':vin8,'Date':s(f.get('date','')),'Customer':s(manual.get('Customer','')),
            'code':s(manual.get('code','')),'Vessel':s(manual.get('Vessel','')),
            'remark':remark_join(records)}
    for i,r in enumerate(records[:7],1):
        flds['Area'+str(i)]=s(r['damage_part_code'])
        flds['TD'+str(i)]=s(r['damage_type_code'])
        flds['SD'+str(i)]=s(r['damage_extent_code'])
    return flds

def build_FORD(vin, records, manual):
    f = records[0]
    flds = {'Vin':s(vin),'Model':s(f.get('model','')),'Date':s(f.get('date','')),
            'Carrier':s(manual.get('Carrier','')),'Vessel':s(manual.get('Vessel','')),
            'Receptor':s(manual.get('Receptor','')),'Remark':remark_join(records)}
    for i,r in enumerate(records[:7],1):
        flds['Part'+str(i)]=s(r['damage_part_code'])
        flds['Code'+str(i)]=s(r['damage_type_code'])
        flds['Severity'+str(i)]=s(r['damage_extent_code'])
    return flds

def build_LINKCO(vin, records, manual):
    f = records[0]
    flds = {'VIN':s(vin),'Model':s(f.get('model','')),'Date':s(f.get('date','')),
            'Location':s(f.get('location','')),'Customer':s(manual.get('Customer','')),
            'DeliveryPart':s(manual.get('Vessel','')),'Vessel':s(manual.get('Vessel','')),
            'Remark':remark_join(records)}
    for i,r in enumerate(records[:3],1):
        flds['Location'+str(i)]=s(r['damage_part_code'])
        flds['Code'+str(i)]=s(r['damage_type_code'])
    return flds

def build_RENAULT(vin, records, manual):
    f = records[0]
    flds = {'VIN':s(vin),'Modelo':s(f.get('model','')),'Date':s(f.get('date','')),
            'Origin':s(f.get('location','')),'Destination':s(manual.get('Destination','')),
            'Customer':s(manual.get('Customer','')),'Location':s(f.get('location','')),
            'N Parte':s(manual.get('N Parte','')),'Remark':remark_join(records)}
    for i,r in enumerate(records[:5],1):
        flds['Part'+str(i)]=s(r['damage_part_code'])
        flds['Code'+str(i)]=s(r['damage_type_code'])
        flds['Gravedad'+str(i)]=s(r.get('damage_classification',''))
    return flds

def build_STELLANTIS(vin, records, manual):
    f = records[0]
    tr = [r for r in records if is_stellantis_transport(r.get('damage_classification',''))]
    ot = [r for r in records if not is_stellantis_transport(r.get('damage_classification',''))]
    rem = []
    for r in ot:
        cls  = s(r.get('damage_classification',''))
        part = s(r['damage_part_code'])
        typ  = s(r['damage_type_code'])
        size = s(r.get('damage_extent',''))
        rmk  = s(r.get('damage_remark',''))
        # Build: Part/Type/Size - Remark
        entry = ' '.join(filter(None,[part,typ,size]))
        if rmk: entry += ' - ' + rmk
        if entry: rem.append(entry)
    for r in tr:
        rmk=s(r.get('damage_remark',''))
        if rmk: rem.append(rmk)
    flds = {'VIN':s(vin),'Model':s(f.get('model','')),'Date':s(f.get('date','')),
            'Location':s(f.get('location','')),'Vessel':s(manual.get('Vessel','')),
            'Refuse':s(manual.get('Refuse','')),'n':str(len(tr)) if tr else '0',
            'remark':' | '.join(rem)}
    for i,r in enumerate(tr[:9],1):
        flds['Part'+str(i)]=s(r['damage_part_code'])
        flds['Damage'+str(i)]=s(r['damage_type_code'])
        flds['Grid'+str(i)]=''   # do NOT write classification in LD column
        flds['Size'+str(i)]=s(r.get('damage_extent',''))
    return flds

def build_VGED(vin, records, manual):
    f = records[0]
    flds = {'VIN':s(vin),'Modelo':s(f.get('model','')),'Date':s(f.get('date','')),
            'Comp Responsable':s(manual.get('Comp Responsable','')),'Remark':remark_join(records)}
    for i,r in enumerate(records[:4],1):
        flds['Part'+str(i)]=s(r['damage_part_code'])
        flds['Code'+str(i)]=s(r['damage_type_code'])
        flds['Gravedad'+str(i)]=s(r['damage_extent_code'])
    return flds

def build_VOLVO(vin, records, manual):
    f = records[0]
    flds = {'VIN':s(vin),'Model':s(f.get('model','')),'Date':s(f.get('date','')),
            'Location':s(f.get('location','')),'Customer':s(manual.get('Customer','')),
            'DeliveryPart':s(manual.get('DeliveryPart','')),'Vessel':s(manual.get('Vessel','')),
            'Remark':remark_join(records)}
    for i,r in enumerate(records[:3],1):
        flds['Location'+str(i)]=s(r['damage_part_code'])
        flds['Code'+str(i)]=s(r['damage_type_code'])
    return flds

BUILDERS = {'BMW':build_BMW,'ECG':build_ECG,'FCA':build_FCA,'FORD':build_FORD,
            'LINKCO':build_LINKCO,'RENAULT':build_RENAULT,'STELLANTIS':build_STELLANTIS,
            'VGED':build_VGED,'VOLVO':build_VOLVO}
FORMATS_ALL = ['BMW','ECG','FCA','FORD','LINKCO','RENAULT','STELLANTIS','VGED','VOLVO']

bootstrap_jobs()

def group_by_vin(records):
    g = {}
    for r in records:
        v = r.get('vin','?')
        if v not in g: g[v] = []
        g[v].append(r)
    return g

def make_merged_pdf(fmt, target_vins, vin_groups, manual):
    merged = PdfWriter()
    for vin in target_vins:
        if vin not in vin_groups: continue
        flat = get_cached_flat(fmt, vin, vin_groups[vin], manual)
        merged.append(PdfReader(flat))
    out = io.BytesIO(); merged.write(out); out.seek(0)
    return out

def make_individual_zip(fmt, target_vins, vin_groups, manual):
    zip_buf = _spooled_buffer()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for vin in target_vins:
            if vin not in vin_groups: continue
            flat = get_cached_flat(fmt, vin, vin_groups[vin], manual)
            flat = compress_pdf_lossless(flat)
            safe = vin.replace('/','_').replace('\\','_')
            zf.writestr(safe+'.pdf', flat.read())
    zip_buf.seek(0)
    return zip_buf

#  Routes 
@app.route('/login')
def login_page():
    if 'uid' in session:
        return redirect('/')
    with open(os.path.join(STATIC_DIR,'login.html'), encoding='utf-8') as f:
        return f.read()

@app.route('/admin')
@admin_required
def admin_page():
    with open(os.path.join(STATIC_DIR,'admin.html'), encoding='utf-8') as f:
        return f.read()

@app.route('/')
@login_required
def index():
    with open(os.path.join(STATIC_DIR,'index.html'), encoding='utf-8') as f:
        return f.read()

@app.route('/api/templates')
def list_templates():
    return jsonify({fmt: os.path.exists(os.path.join(TEMPLATE_DIR, fname))
                    for fmt, fname in TEMPLATE_FILES.items()})

@app.route('/api/server-status')
def server_status():
    return jsonify(_job_stats())

@app.route('/api/manual-fields/<fmt>')
def get_manual_fields(fmt):
    fields = MANUAL_FIELDS.get(fmt.upper(), [])
    return jsonify({'fields': [{'key':k,'label':l,'type':t} for k,l,t in fields]})

@app.route('/api/manual-fields-all')
def get_manual_fields_all():
    """Returns manual fields for ALL formats grouped."""
    result = {}
    for fmt, flist in MANUAL_FIELDS.items():
        result[fmt] = [{'key':k,'label':l,'type':t} for k,l,t in flist]
    return jsonify(result)

@app.route('/api/auth/login', methods=['POST'])
def api_login():
    data = request.get_json(silent=True) or {}
    ok, msg = do_login(data.get('username',''), data.get('password',''))
    return jsonify({'ok': ok, 'error': msg if not ok else None})

@app.route('/api/auth/logout', methods=['GET','POST'])
def api_logout():
    do_logout()
    if request.method == 'POST':
        return jsonify({'ok': True})
    return redirect('/login')

@app.route('/api/auth/me')
def api_me():
    u = current_user()
    return jsonify(u)

@app.route('/api/logo')
def api_logo():
    # Serve logo as base64 for login/admin pages
    logo_path = os.path.join(STATIC_DIR, 'logo.png')
    if os.path.exists(logo_path):
        with open(logo_path, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode()
        return jsonify({'logo': 'data:image/png;base64,' + b64})
    return jsonify({'logo': None})

@app.route('/api/admin/users', methods=['GET'])
@admin_required
def admin_list_users():
    return jsonify(get_all_users())

@app.route('/api/admin/users', methods=['POST'])
@admin_required
def admin_create_user():
    data = request.get_json(silent=True) or {}
    ok, msg = create_user(data.get('username',''), data.get('password',''),
                          data.get('role','user'), data.get('allowed_formats','*'))
    if ok:
        return jsonify({'ok': True, 'message': msg})
    return jsonify({'ok': False, 'error': msg})

@app.route('/api/admin/users/<int:uid>', methods=['PUT'])
@admin_required
def admin_update_user(uid):
    data = request.get_json(silent=True) or {}
    # Prevent admin from removing their own format access
    cu = current_user()
    if cu.get('uid') == uid and cu.get('role') == 'admin' and 'allowed_formats' in data:
        return jsonify({'ok': False, 'error': 'You cannot change your own format access.'}), 400
    ok, msg = update_user(uid, data)
    return jsonify({'ok': ok, 'message': msg})

@app.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@admin_required
def admin_delete_user(uid):
    delete_user(uid)
    return jsonify({'ok': True})

@app.route('/api/settings/brand-format-map', methods=['GET'])
@login_required
def get_brand_map():
    return jsonify({'map': get_brand_format_map(), 'formats': FORMATS_ALL})

@app.route('/api/admin/settings/brand-format-map', methods=['PUT'])
@admin_required
def update_brand_map():
    data = request.get_json(silent=True) or {}
    ok, msg = save_brand_format_map(data.get('map', {}))
    if not ok:
        return jsonify({'ok': False, 'error': msg}), 400
    return jsonify({'ok': True, 'message': msg, 'map': get_brand_format_map()})

#  Column mappings for different Excel formats 
EXCEL_FORMATS = {
    'internal': {
        'vin':'vin','make':'make','model':'model','date':'date','inspection_date':'date',
        'location':'location','surveyor':'surveyor',
        'damage_part_code':'damage_part_code','damage_type_code':'damage_type_code',
        'damage_extent':'damage_extent',
        'damage_extent_code':'damage_extent_code','damage_classification':'damage_classification',
        'damage_remark':'damage_remark',
    },
    'report': {
        'vehicle_vin':'vin','vehicle_make':'make','vehicle_model':'model',
        'transport_date':'date','location':'location','overland_transporter':'surveyor',
        'damage_part_code':'damage_part_code','damage_type_code':'damage_type_code',
        'damage_extent':'damage_extent',
        'damage_extent_code':'damage_extent_code','damage_classification':'damage_classification',
        'remark':'damage_remark',
    },
}
INTERNAL_COLS = ['vin','make','model','date','location','surveyor',
                 'damage_part_code','damage_type_code','damage_extent','damage_extent_code',
                 'damage_classification','damage_remark']

def detect_excel_format(columns):
    col_set = set(str(c).lower() for c in columns)
    if 'vehicle_vin' in col_set: return 'report'
    if 'vin' in col_set: return 'internal'
    return None

def normalize_records(df, fmt_name):
    mapping = EXCEL_FORMATS[fmt_name]
    rename = {src: dst for src, dst in mapping.items() if src in df.columns}
    df = df.rename(columns=rename).copy()
    for col in INTERNAL_COLS:
        if col not in df.columns:
            df[col] = ''
    df = df[INTERNAL_COLS].copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    df = df.where(pd.notna(df), None)
    return [{k:('' if v is None or str(v) in ('nan','NaT') else str(v))
             for k,v in r.items()} for r in df.to_dict(orient='records')]

@app.route('/api/parse-excel', methods=['POST'])
@login_required
def parse_excel():
    file = request.files.get('file')
    if not file: return jsonify({'error':'No file'}), 400
    try:
        try:
            df = pd.read_excel(file, sheet_name='damage_list')
        except Exception:
            try:
                file.stream.seek(0)
                df = pd.read_excel(file, sheet_name='inspection_list')
            except Exception:
                return jsonify({'error': 'Sheet "damage_list" or "inspection_list" not found in Excel file'}), 400
        fmt = detect_excel_format(df.columns)
        if not fmt:
            return jsonify({'error': 'Unrecognized Excel format. Need: vin (or vehicle_vin), damage_part_code, damage_type_code, damage_extent_code'}), 400
        records = normalize_records(df, fmt)
        return jsonify({'records': records, 'count': len(records), 'format': fmt})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/generate', methods=['POST'])
def generate():
    """Single format, merged PDF (all VINs in one file)."""
    data = request.get_json(silent=True) or {}
    fmt  = data.get('format','').upper()
    if fmt not in BUILDERS: return jsonify({'error':'Unknown format: '+fmt}), 400
    if not is_format_allowed(fmt): return jsonify({'error':'Format not allowed for this user'}), 403
    ok, waited = _start_job_or_queue()
    if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try: tpl = get_template(fmt)
        except FileNotFoundError as e: return jsonify({'error':str(e)}), 404
        recs = filter_damage_records(data.get('records',[]))
        vg = group_by_vin(recs)
        target = data.get('vins',[]) or list(vg.keys())
        if not vg: return jsonify({'error':'No valid damages found'}), 400
        try:
            buf = make_merged_pdf(fmt, target, vg, data.get('manual',{}))
        except Exception as e:
            return jsonify({'error':str(e)}), 500
        buf = compress_pdf_lossless(buf)
        if len(target) == 1:
            safe_vin = target[0].replace('/', '_').replace('\\', '_')
            fname = safe_vin + '.pdf'
        else:
            fname = 'VLDR_' + fmt + '_' + str(len(target)) + 'vins.pdf'
        return send_file(buf, mimetype='application/pdf', as_attachment=True,
                         download_name=fname)
    finally:
        _end_job(start_ts)

@app.route('/api/generate-individual', methods=['POST'])
def generate_individual():
    """Single format, ZIP with one PDF per VIN named by VIN."""
    data = request.get_json(silent=True) or {}
    fmt  = data.get('format','').upper()
    if fmt not in BUILDERS: return jsonify({'error':'Unknown format: '+fmt}), 400
    if not is_format_allowed(fmt): return jsonify({'error':'Format not allowed for this user'}), 403
    ok, waited = _start_job_or_queue()
    if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try: get_template(fmt)
        except FileNotFoundError as e: return jsonify({'error':str(e)}), 404
        recs = filter_damage_records(data.get('records',[]))
        vg = group_by_vin(recs)
        target = data.get('vins',[]) or list(vg.keys())
        if not vg: return jsonify({'error':'No valid damages found'}), 400
        try:
            buf = make_individual_zip(fmt, target, vg, data.get('manual',{}))
        except Exception as e:
            return jsonify({'error':str(e)}), 500
        return send_file(buf, mimetype='application/zip', as_attachment=True,
                         download_name='VLDR_'+fmt+'_individual_'+str(len(target))+'vins.zip')
    finally:
        _end_job(start_ts)

@app.route('/api/generate-all', methods=['POST'])
def generate_all():
    """All formats, merged  ZIP with one merged PDF per format."""
    data = request.get_json(silent=True) or {}
    manuals = data.get('manuals', {})
    recs    = filter_damage_records(data.get('records',[]))
    vg      = group_by_vin(recs)
    target  = data.get('vins',[]) or list(vg.keys())
    if not vg: return jsonify({'error':'No valid damages found'}), 400
    ok, waited = _start_job_or_queue()
    if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        zip_buf = _spooled_buffer()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fmt in BUILDERS:
                if not is_format_allowed(fmt): continue
                try:
                    buf = make_merged_pdf(fmt, target, vg, manuals.get(fmt,{}))
                    buf = compress_pdf_lossless(buf)
                    zf.writestr('VLDR_'+fmt+'_'+str(len(target))+'vins.pdf', buf.read())  # format-level file, VIN inside
                except Exception:
                    pass
        zip_buf.seek(0)
        return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                         download_name='VLDR_ALL_merged_'+str(len(target))+'vins.zip')
    finally:
        _end_job(start_ts)

@app.route('/api/generate-all-individual', methods=['POST'])
def generate_all_individual():
    """All formats, individual  ZIP/{fmt}/VLDR_{fmt}_{VIN}.pdf"""
    data = request.get_json(silent=True) or {}
    manuals = data.get('manuals', {})
    recs    = filter_damage_records(data.get('records',[]))
    vg      = group_by_vin(recs)
    target  = data.get('vins',[]) or list(vg.keys())
    if not vg: return jsonify({'error':'No valid damages found'}), 400
    ok, waited = _start_job_or_queue()
    if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        zip_buf = _spooled_buffer()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fmt in BUILDERS:
                if not is_format_allowed(fmt): continue
                try:
                    manual = manuals.get(fmt,{})
                    for vin in target:
                        if vin not in vg: continue
                        # Avoid cache growth during massive all-format exports.
                        flat = get_uncached_flat(fmt, vin, vg[vin], manual)
                        flat = compress_pdf_lossless(flat)
                        safe = vin.replace('/','_').replace('\\','_')
                        zf.writestr(fmt+'/'+safe+'.pdf', flat.read())  # VIN.pdf only
                except Exception:
                    pass
        zip_buf.seek(0)
        return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                         download_name='VLDR_ALL_individual_'+str(len(target))+'vins.zip')
    finally:
        _end_job(start_ts)


@app.route('/api/preview', methods=['POST'])
def preview():
    """Generate a single-page PDF preview for one VIN in one format."""
    data = request.get_json(silent=True) or {}
    fmt    = data.get('format', '').upper()
    vin    = data.get('vin', '')
    recs   = data.get('records', [])
    manual = data.get('manual', {})

    if fmt not in BUILDERS:
        return jsonify({'error': 'Unknown format: ' + fmt}), 400
    if not is_format_allowed(fmt):
        return jsonify({'error': 'Format not allowed for this user'}), 403
    try:
        tpl = get_template(fmt)
    except FileNotFoundError as e:
        return jsonify({'error': str(e)}), 404

    recs = filter_damage_records(recs)
    vg = group_by_vin(recs)
    if vin not in vg:
        return jsonify({'error': 'VIN not found: ' + vin}), 404

    ok, waited = _start_job_or_queue()
    if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try:
            flat = get_cached_flat(fmt, vin, vg[vin], manual)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    finally:
        _end_job(start_ts)

    import base64
    b64 = base64.b64encode(flat.read()).decode('utf-8')
    return jsonify({'pdf_b64': b64})


@app.route('/api/generate-batch', methods=['POST'])
def generate_batch():
    """
    Generate PDFs for a list of {vin, format, manual} entries.
    mode: 'merged' -> one PDF per format group
          'individual' -> ZIP with one PDF per VIN named VLDR_{fmt}_{VIN}.pdf
          'all-merged' -> ZIP with one merged PDF per format
    """
    data = request.get_json(silent=True) or {}
    items  = data.get('items', [])   # [{vin, format, manual}]
    mode   = data.get('mode', 'individual')
    recs   = filter_damage_records(data.get('records', []))
    vg     = group_by_vin(recs)

    if mode == 'individual':
        ok, waited = _start_job_or_queue()
        if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
        start_ts = time.time()
        try:
            zip_buf = _spooled_buffer()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for item in items:
                    fmt    = item.get('format', '').upper()
                    vin    = item.get('vin', '')
                    manual = item.get('manual', {})
                    if fmt not in BUILDERS or vin not in vg:
                        continue
                    if not is_format_allowed(fmt):
                        continue
                    try:
                        # Batch mode: avoid storing every PDF in LRU cache (OOM on small instances).
                        flat = get_uncached_flat(fmt, vin, vg[vin], manual)  # flatten = non-editable
                        flat = compress_pdf_lossless(flat)
                        safe = vin.replace('/', '_').replace('\\', '_')
                        zf.writestr(safe + '.pdf', flat.read()) # VIN.pdf only
                    except Exception:
                        pass
            zip_buf.seek(0)
            total = len(items)
            return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                             download_name='VLDR_selected_' + str(total) + 'vins.zip')
        finally:
            _end_job(start_ts)

    elif mode == 'all-merged':
        ok, waited = _start_job_or_queue()
        if not ok: return jsonify({'error':'Server busy. Try again shortly.'}), 429
        start_ts = time.time()
        try:
            from collections import defaultdict
            by_fmt = defaultdict(list)
            manual_by_fmt = {}
            for item in items:
                fmt = item.get('format','').upper()
                by_fmt[fmt].append(item.get('vin',''))
                manual_by_fmt[fmt] = item.get('manual', {})

            zip_buf = _spooled_buffer()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fmt, vins in by_fmt.items():
                    if fmt not in BUILDERS: continue
                    if not is_format_allowed(fmt): continue
                    try:
                        merged = PdfWriter()
                        for vin in vins:
                            if vin not in vg: continue
                            flat = get_uncached_flat(fmt, vin, vg[vin], manual_by_fmt.get(fmt,{}))
                            merged.append(PdfReader(flat))
                        pdf_out = _spooled_buffer()
                        merged.write(pdf_out)
                        pdf_out = compress_pdf_lossless(pdf_out)
                        zf.writestr('VLDR_' + fmt + '_' + str(len(vins)) + 'vins.pdf', pdf_out.read())
                    except Exception:
                        pass
            zip_buf.seek(0)
            return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                             download_name='VLDR_all_formats.zip')
        finally:
            _end_job(start_ts)

    else:
        return jsonify({'error': 'Unknown mode'}), 400

@app.route('/api/generate-batch-async', methods=['POST'])
def generate_batch_async():
    """Queue batch generation and return job id immediately."""
    if not _jobs_bootstrapped:
        bootstrap_jobs()
    if not _jobs_available:
        return jsonify({'error': 'Async job queue unavailable on this instance'}), 503
    data = request.get_json(silent=True) or {}
    items = data.get('items', []) or []
    mode = data.get('mode', 'individual')
    if mode not in ('individual', 'all-merged'):
        return jsonify({'error': 'Unknown mode'}), 400
    if not items:
        return jsonify({'error': 'No items to process'}), 400
    cu = current_user()
    job_id = create_job({
        'items': items,
        'mode': mode,
        'records': data.get('records', []) or [],
        'allowed_formats': cu.get('allowed_formats')
    })
    return jsonify({'ok': True, 'job_id': job_id})

@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    if not _jobs_available:
        return jsonify({'error': 'Async job queue unavailable on this instance'}), 503
    row = get_job(job_id)
    if not row:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify({
        'id': row['id'],
        'status': row['status'],
        'progress': row['progress'] or 0,
        'message': row['message'] or '',
        'error': row['error'],
        'total_items': row['total_items'] or 0,
        'done_items': row['done_items'] or 0
    })

@app.route('/api/jobs/<job_id>/download', methods=['GET'])
def download_job_result(job_id):
    if not _jobs_available:
        return jsonify({'error': 'Async job queue unavailable on this instance'}), 503
    row = get_job(job_id)
    if not row:
        return jsonify({'error': 'Job not found'}), 404
    if row['status'] != 'done':
        return jsonify({'error': 'Job is not ready yet'}), 409
    path = row['result_path'] or ''
    if not path or not os.path.exists(path):
        return jsonify({'error': 'Result file not found'}), 404
    name = row['result_name'] or ('VLDR_' + job_id + '.zip')
    return send_file(path, mimetype='application/zip', as_attachment=True, download_name=name)


if __name__ == '__main__':
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
    bootstrap_jobs()
    missing = [f for f,n in TEMPLATE_FILES.items()
               if not os.path.exists(os.path.join(TEMPLATE_DIR, n))]
    # Init auth database
    init_db()

    # Check pdftk availability
    import subprocess as _sp
    try:
        _sp.run(['pdftk', '--version'], capture_output=True, check=True)
        print('  pdftk: found')
    except Exception:
        print('  WARNING: pdftk not found! PDF flattening will fail.')
        print('  Install from: https://www.pdflabs.com/tools/pdftk-the-pdf-toolkit/')

    print('\n  VLDR Generator  ->  http://localhost:5050')
    print('  Templates: ' + TEMPLATE_DIR)
    if missing: print('  MISSING templates: ' + str(missing))
    else:       print('  All 9 templates found.')

    import os as _os
    port = int(_os.environ.get('PORT', 5050))
    is_local = port == 5050 and os.environ.get('NO_BROWSER', '0') != '1'

    if is_local:
        import threading, webbrowser
        def open_browser():
            import time
            time.sleep(1.5)
            webbrowser.open(f'http://127.0.0.1:{port}')
        threading.Thread(target=open_browser, daemon=True).start()

    app.run(debug=False, port=port, host='0.0.0.0')








