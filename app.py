"""
VLDR Generator - Flask Backend
Run: python app.py  |  Open: http://localhost:5050
Place PDF templates in ./templates/ folder (same directory as app.py)
"""
import os, io, zipfile, json, base64, time, threading
from collections import deque
from datetime import timedelta
from flask import Flask, request, jsonify, send_file, session, redirect
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, create_string_object, DictionaryObject
import pandas as pd

app = Flask(__name__)
# SECRET_KEY must be set in environment for production (Render sets it automatically)
_default_key = 'vldr-local-dev-' + __import__('hashlib').md5(b'vldr').hexdigest()
app.secret_key = os.environ.get('SECRET_KEY', _default_key)
app.permanent_session_lifetime = timedelta(hours=8)

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
STATIC_DIR   = os.path.join(BASE_DIR, 'static')

# Concurrency control (Render free is tight)
MAX_JOBS = int(os.environ.get('VLDR_MAX_JOBS', '2'))
_job_sem = threading.BoundedSemaphore(MAX_JOBS)
_job_lock = threading.Lock()
_active_jobs = 0
_recent_times = deque(maxlen=30)

def _try_start_job():
    global _active_jobs
    if not _job_sem.acquire(blocking=False):
        return False
    with _job_lock:
        _active_jobs += 1
    return True

def _end_job(start_ts):
    global _active_jobs
    with _job_lock:
        _active_jobs = max(0, _active_jobs - 1)
    _job_sem.release()
    _recent_times.append(time.time() - start_ts)

def _job_stats():
    with _job_lock:
        active = _active_jobs
    avg = sum(_recent_times) / len(_recent_times) if _recent_times else 0
    return {'active': active, 'max': MAX_JOBS, 'avg_sec': round(avg, 2)}
# Auth - import after BASE_DIR is defined
from auth import init_db, do_login, do_logout, current_user
from auth import login_required, admin_required, get_all_users, create_user, update_user, delete_user

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

# ── Helpers ──────────────────────────────────────────────
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

# ── Builders ─────────────────────────────────────────────
def build_BMW(vin, records, manual):
    f = records[0]
    dp = s(f.get('date','')).split('-')
    yr,mo,dy = (dp+['','',''])[:3]
    flds = {'Vin':s(vin),'Dia':dy,'Mes':mo,'Ano':yr,'Año':yr,
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
        size = s(r['damage_extent_code'])
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
        flds['Size'+str(i)]=s(r['damage_extent_code'])
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

def group_by_vin(records):
    g = {}
    for r in records:
        v = r.get('vin','?')
        if v not in g: g[v] = []
        g[v].append(r)
    return g

def make_merged_pdf(fmt, target_vins, vin_groups, manual):
    tpl = get_template(fmt)
    fsz = FONT_SIZES.get(fmt, {})
    comb_skip = comb_skip_for(fmt)
    merged = PdfWriter()
    for vin in target_vins:
        if vin not in vin_groups: continue
        flat = fill_pdf_and_overlay_comb(tpl, BUILDERS[fmt](vin, vin_groups[vin], manual), fsz, comb_skip=comb_skip)
        merged.append(PdfReader(flat))
    out = io.BytesIO(); merged.write(out); out.seek(0)
    return out

def make_individual_zip(fmt, target_vins, vin_groups, manual):
    tpl = get_template(fmt)
    fsz = FONT_SIZES.get(fmt, {})
    comb_skip = comb_skip_for(fmt)
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for vin in target_vins:
            if vin not in vin_groups: continue
            flat = fill_pdf_and_overlay_comb(tpl, BUILDERS[fmt](vin, vin_groups[vin], manual), fsz, comb_skip=comb_skip)
            safe = vin.replace('/','_').replace('\\','_')
            zf.writestr(safe+'.pdf', flat.read())
    zip_buf.seek(0)
    return zip_buf

# ── Routes ───────────────────────────────────────────────
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
    data = request.json or {}
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
    data = request.json or {}
    ok, msg = create_user(data.get('username',''), data.get('password',''),
                          data.get('role','user'), data.get('allowed_formats','*'))
    if ok:
        return jsonify({'ok': True, 'message': msg})
    return jsonify({'ok': False, 'error': msg})

@app.route('/api/admin/users/<int:uid>', methods=['PUT'])
@admin_required
def admin_update_user(uid):
    data = request.json or {}
    # Prevent admin from removing their own format access
    cu = current_user()
    if cu.get('uid') == uid and cu.get('role') == 'admin' and 'allowed_formats' in data:
        return jsonify({'ok': False, 'error': 'You cannot change your own format access.'}), 400
    ok, msg = update_user(uid, request.json or {})
    return jsonify({'ok': ok, 'message': msg})

@app.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@admin_required
def admin_delete_user(uid):
    delete_user(uid)
    return jsonify({'ok': True})

# ── Column mappings for different Excel formats ──────────
EXCEL_FORMATS = {
    'internal': {
        'vin':'vin','make':'make','model':'model','date':'date',
        'location':'location','surveyor':'surveyor',
        'damage_part_code':'damage_part_code','damage_type_code':'damage_type_code',
        'damage_extent_code':'damage_extent_code','damage_classification':'damage_classification',
        'damage_remark':'damage_remark',
    },
    'report': {
        'vehicle_vin':'vin','vehicle_make':'make','vehicle_model':'model',
        'transport_date':'date','location':'location','overland_transporter':'surveyor',
        'damage_part_code':'damage_part_code','damage_type_code':'damage_type_code',
        'damage_extent_code':'damage_extent_code','damage_classification':'damage_classification',
        'remark':'damage_remark',
    },
}
INTERNAL_COLS = ['vin','make','model','date','location','surveyor',
                 'damage_part_code','damage_type_code','damage_extent_code',
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
            return jsonify({'error': 'Sheet "damage_list" not found in Excel file'}), 400
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
    data = request.json
    fmt  = data.get('format','').upper()
    if fmt not in BUILDERS: return jsonify({'error':'Unknown format: '+fmt}), 400
    if not is_format_allowed(fmt): return jsonify({'error':'Format not allowed for this user'}), 403
    if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try: tpl = get_template(fmt)
        except FileNotFoundError as e: return jsonify({'error':str(e)}), 404
        vg = group_by_vin(data.get('records',[]))
        target = data.get('vins',[]) or list(vg.keys())
        try:
            buf = make_merged_pdf(fmt, target, vg, data.get('manual',{}))
        except Exception as e:
            return jsonify({'error':str(e)}), 500
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
    data = request.json
    fmt  = data.get('format','').upper()
    if fmt not in BUILDERS: return jsonify({'error':'Unknown format: '+fmt}), 400
    if not is_format_allowed(fmt): return jsonify({'error':'Format not allowed for this user'}), 403
    if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try: get_template(fmt)
        except FileNotFoundError as e: return jsonify({'error':str(e)}), 404
        vg = group_by_vin(data.get('records',[]))
        target = data.get('vins',[]) or list(vg.keys())
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
    """All formats, merged — ZIP with one merged PDF per format."""
    data    = request.json
    manuals = data.get('manuals', {})
    vg      = group_by_vin(data.get('records',[]))
    target  = data.get('vins',[]) or list(vg.keys())
    if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fmt in BUILDERS:
                if not is_format_allowed(fmt): continue
                try:
                    buf = make_merged_pdf(fmt, target, vg, manuals.get(fmt,{}))
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
    """All formats, individual — ZIP/{fmt}/VLDR_{fmt}_{VIN}.pdf"""
    data    = request.json
    manuals = data.get('manuals', {})
    vg      = group_by_vin(data.get('records',[]))
    target  = data.get('vins',[]) or list(vg.keys())
    if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fmt in BUILDERS:
                if not is_format_allowed(fmt): continue
                try:
                    tpl = get_template(fmt)
                    fsz = FONT_SIZES.get(fmt,{})
                    manual = manuals.get(fmt,{})
                    comb_skip = comb_skip_for(fmt)
                    for vin in target:
                        if vin not in vg: continue
                        flat = fill_pdf_and_overlay_comb(
                            tpl,
                            BUILDERS[fmt](vin, vg[vin], manual),
                            fsz,
                            comb_skip=comb_skip
                        )
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
    data   = request.json
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

    vg = group_by_vin(recs)
    if vin not in vg:
        return jsonify({'error': 'VIN not found: ' + vin}), 404

    if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
    start_ts = time.time()
    try:
        try:
            comb_skip = comb_skip_for(fmt)
            flat = fill_pdf_and_overlay_comb(tpl, BUILDERS[fmt](vin, vg[vin], manual),
                                             FONT_SIZES.get(fmt, {}), comb_skip=comb_skip)
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
    data   = request.json
    items  = data.get('items', [])   # [{vin, format, manual}]
    mode   = data.get('mode', 'individual')
    recs   = data.get('records', [])
    vg     = group_by_vin(recs)

    if mode == 'individual':
        if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
        start_ts = time.time()
        try:
            zip_buf = io.BytesIO()
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
                        tpl = get_template(fmt)
                        fsz = FONT_SIZES.get(fmt, {})
                        comb_skip = comb_skip_for(fmt)
                        flat = fill_pdf_and_overlay_comb(
                            tpl,
                            BUILDERS[fmt](vin, vg[vin], manual),
                            fsz,
                            comb_skip=comb_skip
                        )          # flatten = non-editable
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
        if not _try_start_job(): return jsonify({'error':'Server busy. Try again shortly.'}), 429
        start_ts = time.time()
        try:
            from collections import defaultdict
            by_fmt = defaultdict(list)
            manual_by_fmt = {}
            for item in items:
                fmt = item.get('format','').upper()
                by_fmt[fmt].append(item.get('vin',''))
                manual_by_fmt[fmt] = item.get('manual', {})

            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fmt, vins in by_fmt.items():
                    if fmt not in BUILDERS: continue
                    if not is_format_allowed(fmt): continue
                    try:
                        tpl = get_template(fmt)
                        fsz = FONT_SIZES.get(fmt, {})
                        merged = PdfWriter()
                        comb_skip = comb_skip_for(fmt)
                        for vin in vins:
                            if vin not in vg: continue
                            flat = fill_pdf_and_overlay_comb(
                                tpl,
                                BUILDERS[fmt](vin, vg[vin], manual_by_fmt.get(fmt,{})),
                                fsz,
                                comb_skip=comb_skip
                            )
                            merged.append(PdfReader(flat))
                        pdf_out = io.BytesIO(); merged.write(pdf_out)
                        zf.writestr('VLDR_' + fmt + '_' + str(len(vins)) + 'vins.pdf', pdf_out.getvalue())
                    except Exception:
                        pass
            zip_buf.seek(0)
            return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                             download_name='VLDR_all_formats.zip')
        finally:
            _end_job(start_ts)

    else:
        return jsonify({'error': 'Unknown mode'}), 400


if __name__ == '__main__':
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
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
    is_local = port == 5050

    if is_local:
        import threading, webbrowser
        def open_browser():
            import time
            time.sleep(1.5)
            webbrowser.open(f'http://127.0.0.1:{port}')
        threading.Thread(target=open_browser, daemon=True).start()

    app.run(debug=False, port=port, host='0.0.0.0')
