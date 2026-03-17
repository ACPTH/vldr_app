"""
auth.py - User authentication for VLDR Generator
SQLite + Flask sessions. No external dependencies beyond Flask.
"""
import sqlite3, hashlib, secrets, os
from functools import wraps
from flask import session, redirect, request, jsonify

DB_PATH = os.environ.get('VLDR_DB_PATH') or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'users.db')

# ── DB ────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute('''CREATE TABLE IF NOT EXISTS users (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        salt     TEXT NOT NULL,
        role     TEXT NOT NULL DEFAULT 'user',
        active   INTEGER NOT NULL DEFAULT 1,
        allowed_formats TEXT DEFAULT '*',
        created  TEXT DEFAULT (datetime('now'))
    )''')
    # Migrate: add allowed_formats if missing (older DBs)
    cols = [r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()]
    if 'allowed_formats' not in cols:
        con.execute("ALTER TABLE users ADD COLUMN allowed_formats TEXT DEFAULT '*'")
        con.execute("UPDATE users SET allowed_formats='*' WHERE allowed_formats IS NULL")
    con.commit()
    if con.execute('SELECT COUNT(*) FROM users').fetchone()[0] == 0:
        _insert_user(con, 'admin', 'admin1234', 'admin', '*')
        print('  Default admin: admin / admin1234')
        print('  Change this password in Admin panel after first login!')
    con.close()

def _hash(pw, salt):
    return hashlib.pbkdf2_hmac('sha256', pw.encode(), salt.encode(), 260000).hex()

def _insert_user(con, username, password, role, allowed_formats):
    salt = secrets.token_hex(16)
    con.execute('INSERT INTO users (username,password,salt,role,allowed_formats) VALUES (?,?,?,?,?)',
                (username.lower().strip(), _hash(password, salt), salt, role, allowed_formats))
    con.commit()

# ── Auth actions ──────────────────────────────────────────
def do_login(username, password):
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        'SELECT id,password,salt,role,active FROM users WHERE username=?',
        (username.lower().strip(),)).fetchone()
    con.close()
    if not row:           return False, 'User not found'
    uid, pw_h, salt, role, active = row
    if not active:        return False, 'Account disabled'
    if _hash(password, salt) != pw_h: return False, 'Wrong password'
    session.permanent   = True
    session['uid']      = uid
    session['username'] = username.lower().strip()
    session['role']     = role
    return True, 'OK'

def do_logout():
    session.clear()

def current_user():
    af = get_user_allowed_formats(session.get('uid')) if session.get('uid') else None
    return {
        'uid':       session.get('uid'),
        'username':  session.get('username'),
        'role':      session.get('role'),
        'allowed_formats': af,
        'logged_in': 'uid' in session,
    }

# ── User management ───────────────────────────────────────
def get_all_users():
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        'SELECT id,username,role,active,created,allowed_formats FROM users ORDER BY id').fetchall()
    con.close()
    return [{'id':r[0],'username':r[1],'role':r[2],
             'active':bool(r[3]),'created':r[4],
             'allowed_formats': _parse_allowed_formats(r[5])} for r in rows]

def create_user(username, password, role='user', allowed_formats='*'):
    if not username or not password:  return False, 'Username and password required'
    if len(password) < 6:             return False, 'Password must be at least 6 characters'
    try:
        con = sqlite3.connect(DB_PATH)
        _insert_user(con, username, password, role, _serialize_allowed_formats(allowed_formats))
        con.close()
        return True, 'User created'
    except sqlite3.IntegrityError:
        return False, 'Username already exists'

def update_user(uid, data):
    con = sqlite3.connect(DB_PATH)
    if data.get('password'):
        salt = secrets.token_hex(16)
        pw   = _hash(data['password'], salt)
        con.execute('UPDATE users SET password=?,salt=? WHERE id=?', (pw, salt, uid))
    if 'active' in data:
        con.execute('UPDATE users SET active=? WHERE id=?',
                    (1 if data['active'] else 0, uid))
    if 'role' in data and data['role'] in ('user', 'admin'):
        con.execute('UPDATE users SET role=? WHERE id=?', (data['role'], uid))
    if 'allowed_formats' in data:
        con.execute('UPDATE users SET allowed_formats=? WHERE id=?',
                    (_serialize_allowed_formats(data['allowed_formats']), uid))
    con.commit(); con.close()
    return True, 'Updated'

def _parse_allowed_formats(val):
    if not val or val == '*':
        return None
    parts = [p.strip().upper() for p in str(val).split(',')]
    return [p for p in parts if p]

def _serialize_allowed_formats(val):
    if val is None or val == '' or val == '*':
        return '*'
    if isinstance(val, list):
        parts = [str(p).strip().upper() for p in val if str(p).strip()]
        return ','.join(parts) if parts else '*'
    # String input
    parts = [p.strip().upper() for p in str(val).split(',')]
    parts = [p for p in parts if p]
    return ','.join(parts) if parts else '*'

def get_user_allowed_formats(uid):
    if not uid: return None
    con = sqlite3.connect(DB_PATH)
    row = con.execute('SELECT allowed_formats FROM users WHERE id=?', (uid,)).fetchone()
    con.close()
    if not row: return None
    return _parse_allowed_formats(row[0])

def delete_user(uid):
    con = sqlite3.connect(DB_PATH)
    con.execute('DELETE FROM users WHERE id=? AND username != ?', (uid, 'admin'))
    con.commit(); con.close()

# auto-init on import
try:
    init_db()
except Exception:
    pass

# ── Decorators ────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'uid' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Not authenticated'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'uid' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Not authenticated'}), 401
            return redirect('/login')
        if session.get('role') != 'admin':
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Admin required'}), 403
            return redirect('/')
        return f(*args, **kwargs)
    return decorated
