from __future__ import annotations
import json, os, shutil, threading, time, uuid, secrets, hmac
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import chayapp as core

BASE = Path(__file__).resolve().parent
STATIC = BASE / 'static'
MEDIA_DIR = core.HOME / 'web_uploads'
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
STORE = core.Store()

# Personal access token: generated once on this computer and kept beside server.py.
TOKEN_FILE = BASE / '.postflow_token'
def load_or_create_api_token() -> str:
    try:
        existing = TOKEN_FILE.read_text(encoding='utf-8').strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass
    token = secrets.token_urlsafe(48)
    # Exclusive creation avoids accidentally replacing an existing token.
    try:
        with TOKEN_FILE.open('x', encoding='utf-8') as f:
            f.write(token + '\\n')
    except FileExistsError:
        existing = TOKEN_FILE.read_text(encoding='utf-8').strip()
        if existing:
            return existing
        TOKEN_FILE.write_text(token + '\\n', encoding='utf-8')
    return token

API_TOKEN = load_or_create_api_token()

app = FastAPI(title='PostFlow Web', version='1.1.0')
allowed_origins = [x.strip() for x in os.getenv('POSTFLOW_ALLOWED_ORIGINS', '*').split(',') if x.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ['*'],
    allow_credentials=False,
    allow_methods=['GET', 'POST', 'DELETE', 'OPTIONS'],
    allow_headers=['Content-Type', 'X-PostFlow-Token'],
)
app.mount('/static', StaticFiles(directory=STATIC), name='static')

@app.middleware('http')
async def require_api_token(request: Request, call_next):
    # CORS preflight has no token; actual API requests must always authenticate.
    if request.url.path.startswith('/api/') and request.method != 'OPTIONS':
        supplied = request.headers.get('X-PostFlow-Token', '')
        if not supplied or not hmac.compare_digest(supplied, API_TOKEN):
            return JSONResponse(status_code=401, content={'detail': 'API token không hợp lệ hoặc chưa được cung cấp. Hãy kiểm tra Cài đặt kết nối PostFlow.'})
    return await call_next(request)

state_lock = threading.RLock()
worker_lock = threading.Lock()
login_state = {'status': 'unknown', 'message': 'Chưa kiểm tra phiên Facebook'}
login_confirm = threading.Event()
login_thread: threading.Thread | None = None
publish_thread: threading.Thread | None = None
publish_stop = threading.Event()
progress_events: list[dict[str, Any]] = []
MAX_UPLOAD = 500 * 1024 * 1024
ALLOWED = {'.jpg', '.jpeg', '.png', '.webp'}


def log(message: str, category: str = 'Đăng bài') -> None:
    item = {'time': datetime.now().astimezone().isoformat(timespec='seconds'), 'message': str(message), 'category': category}
    with state_lock:
        progress_events.append(item)
        del progress_events[:-500]
    try:
        STORE.add_activity(category, str(message))
    except Exception:
        pass


def job_dict(job: core.Job) -> dict:
    return {'id': job.id, 'group_url': job.group_url, 'content': job.content,
            'images': job.images, 'status': job.status, 'status_label': core.LABELS.get(job.status, job.status),
            'note': job.note, 'sent_intent_at': job.sent_intent_at,
            'created_at': job.created_at, 'permalink': job.permalink,
            'evidence_path': job.evidence_path}


def is_login_ready(page) -> bool:
    try:
        cookies = page.context.cookies(['https://www.facebook.com'])
        return any(c.get('name') == 'c_user' and c.get('value') for c in cookies)
    except Exception:
        return False


def login_runner():
    global login_state
    try:
        from playwright.sync_api import sync_playwright
        core.BROWSER_DIR.mkdir(parents=True, exist_ok=True)
        login_confirm.clear()
        with sync_playwright() as pw:
            ctx = pw.chromium.launch_persistent_context(
                str(core.BROWSER_DIR), headless=False, locale='vi-VN',
                viewport={'width': 1280, 'height': 850}, timeout=25000)
            try:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                page.goto('https://www.facebook.com/', wait_until='domcontentloaded', timeout=30000)
                with state_lock:
                    login_state = {'status': 'waiting', 'message': 'Cửa sổ Chromium đã mở. Hãy tự đăng nhập Facebook rồi quay lại bấm “Tôi đã đăng nhập”.'}
                log('Đã mở cửa sổ đăng nhập Facebook. Không nhập mật khẩu vào PostFlow.', 'Facebook')
                login_confirm.wait(timeout=900)
                ok = is_login_ready(page)
                with state_lock:
                    login_state = {'status': 'connected' if ok else 'not_verified',
                                   'message': 'Đã xác nhận phiên Facebook trên trình duyệt.' if ok else 'Chưa tìm thấy cookie đăng nhập c_user. Hãy kiểm tra lại Facebook rồi thử xác nhận.'}
                log(login_state['message'], 'Facebook')
            finally:
                ctx.close()
    except Exception as exc:
        with state_lock:
            login_state = {'status': 'error', 'message': f'{type(exc).__name__}: {str(exc)[:300]}'}
        log('Không mở được trình duyệt đăng nhập: ' + str(exc), 'Lỗi')


class QueueRequest(BaseModel):
    content: str
    groups: list[str]
    image_paths: list[str] = []
    delay_seconds: int = 15

class GroupRequest(BaseModel):
    groups: list[str]

class ScheduleRequest(BaseModel):
    title: str = 'Bài đăng theo lịch'
    content: str
    groups: list[str]
    image_paths: list[str] = []
    scheduled_at: str
    delay_seconds: int = 15

class IdRequest(BaseModel):
    id: int


@app.get('/')
def index():
    return FileResponse(STATIC / 'index.html')

@app.get('/api/status')
def status():
    with state_lock:
        login = dict(login_state)
    return {'login': login, 'worker_running': bool(publish_thread and publish_thread.is_alive()),
            'home': str(core.HOME), 'max_images': core.MAX_IMAGES, 'max_upload_mb': 500}

@app.post('/api/login/start')
def login_start():
    global login_thread
    if login_thread and login_thread.is_alive():
        return {'ok': True, 'message': 'Cửa sổ đăng nhập đang mở.'}
    with state_lock:
        login_state.update(status='starting', message='Đang mở Chromium...')
    login_thread = threading.Thread(target=login_runner, daemon=True)
    login_thread.start()
    return {'ok': True, 'message': 'Đang mở cửa sổ đăng nhập Facebook.'}

@app.post('/api/login/confirm')
def login_done():
    if not login_thread or not login_thread.is_alive():
        raise HTTPException(409, 'Không có phiên đăng nhập đang mở.')
    login_confirm.set()
    return {'ok': True, 'message': 'Đã gửi yêu cầu xác nhận đăng nhập.'}

@app.post('/api/login/logout')
def logout():
    # Do not delete browser profile; Facebook logout should be done in its own browser window.
    with state_lock:
        login_state.update(status='unknown', message='Hãy đăng xuất trực tiếp trong cửa sổ Facebook nếu cần.')
    return {'ok': True, 'message': 'Trạng thái đã được đặt lại. Đăng xuất Facebook trực tiếp trong trình duyệt.'}

@app.get('/api/groups')
def get_groups():
    raw = STORE.get_app_state('web_groups') or ''
    return {'groups': raw.splitlines() if raw else []}

@app.post('/api/groups')
def save_groups(req: GroupRequest):
    cleaned = []
    seen = set()
    for raw in req.groups:
        if not raw.strip(): continue
        url = core.clean_group(raw)
        if url not in seen:
            cleaned.append(url); seen.add(url)
    STORE.set_app_state('web_groups', '\n'.join(cleaned))
    return {'ok': True, 'groups': cleaned}

@app.post('/api/upload')
async def upload(files: list[UploadFile] = File(...)):
    if len(files) > core.MAX_IMAGES:
        raise HTTPException(400, f'Tối đa {core.MAX_IMAGES} ảnh mỗi lần tải.')
    saved = []
    for f in files:
        ext = Path(f.filename or '').suffix.lower()
        if ext not in ALLOWED:
            raise HTTPException(400, f'Định dạng không hỗ trợ: {f.filename}')
        name = f'{uuid.uuid4().hex}{ext}'
        dest = MEDIA_DIR / name
        size = 0
        try:
            with dest.open('wb') as out:
                while chunk := await f.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_UPLOAD:
                        raise HTTPException(413, f'{f.filename} vượt quá 500 MB.')
                    out.write(chunk)
        except Exception:
            dest.unlink(missing_ok=True)
            raise
        saved.append(str(dest.resolve()))
    return {'files': saved}


def launch_publish(ids: list[int], delay_seconds: int = 15):
    global publish_thread
    if worker_lock.locked():
        raise HTTPException(409, 'Đang có lượt đăng bài chạy. Hãy đợi lượt hiện tại kết thúc.')
    def work():
        if not worker_lock.acquire(blocking=False): return
        try:
            publish_stop.clear()
            log(f'Bắt đầu đăng {len(ids)} bài/nhóm.', 'Đăng bài')
            with core.browser_batch_session() as page:
                for idx, jid in enumerate(ids):
                    if publish_stop.is_set():
                        log('Đã nhận yêu cầu dừng sau bài hiện tại.', 'Hệ thống'); break
                    job = STORE.get(jid)
                    if job.status != core.QUEUED:
                        continue
                    try:
                        result = core.run_one(job, STORE, lambda m: log(m), page=page)
                        log(f'Nhóm {job.group_url}: {core.LABELS.get(result, result)}', 'Kết quả')
                    except Exception as exc:
                        STORE.transition(jid, core.FAILED, f'{type(exc).__name__}: {str(exc)[:300]}')
                        log(f'Lỗi ở nhóm {job.group_url}: {exc}', 'Lỗi')
                    if idx < len(ids)-1 and not publish_stop.is_set() and delay_seconds > 0:
                        for _ in range(delay_seconds):
                            if publish_stop.is_set(): break
                            time.sleep(1)
            log('Đã kết thúc lượt đăng.', 'Hệ thống')
        except Exception as exc:
            log(f'Không thể chạy trình duyệt: {type(exc).__name__}: {exc}', 'Lỗi')
        finally:
            worker_lock.release()
    publish_thread = threading.Thread(target=work, daemon=True)
    publish_thread.start()

@app.post('/api/posts/publish')
def publish(req: QueueRequest):
    if not req.content.strip(): raise HTTPException(400, 'Nội dung bài viết đang trống.')
    groups = [core.clean_group(g) for g in req.groups]
    if not groups: raise HTTPException(400, 'Hãy chọn ít nhất một nhóm Facebook.')
    images = core.validate_images(req.image_paths)
    ids, duplicates = STORE.add_jobs(req.content, images, groups)
    if not ids: return {'ok': True, 'created': [], 'duplicates': duplicates, 'message': 'Các nhóm này đã có bài trùng nội dung trong lịch sử.'}
    launch_publish(ids, max(0, min(int(req.delay_seconds), 3600)))
    return {'ok': True, 'created': ids, 'duplicates': duplicates, 'message': f'Đã đưa {len(ids)} nhóm vào hàng đợi.'}

@app.post('/api/posts/stop')
def stop_publish():
    publish_stop.set()
    return {'ok': True, 'message': 'Sẽ dừng sau bài đang xử lý.'}

@app.get('/api/jobs')
def jobs():
    return {'jobs': [job_dict(j) for j in STORE.all(1000)]}

@app.post('/api/jobs/confirm')
def confirm_job(req: IdRequest):
    STORE.confirm(req.id); return {'ok': True}

@app.post('/api/jobs/retry')
def retry_job(req: IdRequest):
    if not STORE.retry_before_submit(req.id): raise HTTPException(409, 'Chỉ có thể thử lại khi lỗi chắc chắn xảy ra trước khi nhấn Đăng.')
    launch_publish([req.id], 0); return {'ok': True}

@app.get('/api/activity')
def activity():
    with state_lock: recent = list(reversed(progress_events[-200:]))
    return {'events': recent, 'stored': STORE.list_activity(200)}

@app.get('/api/schedules')
def schedules():
    return {'schedules': STORE.list_schedules(500)}

@app.post('/api/schedules')
def add_schedule(req: ScheduleRequest):
    groups = [core.clean_group(g) for g in req.groups]
    images = core.validate_images(req.image_paths)
    sid = STORE.add_schedule(req.title, req.content, '\n'.join(groups), images, req.scheduled_at, req.delay_seconds)
    return {'ok': True, 'id': sid}

@app.delete('/api/schedules/{sid}')
def cancel_schedule(sid: str):
    if not STORE.cancel_schedule(sid): raise HTTPException(409, 'Lịch không còn ở trạng thái có thể hủy.')
    return {'ok': True}

@app.get('/api/library')
def library():
    return {'posts': STORE.list_library_posts(500)}

class LibraryRequest(BaseModel):
    title: str
    content: str
    groups: list[str]
    image_paths: list[str] = []

@app.post('/api/library')
def save_library(req: LibraryRequest):
    pid = STORE.save_library_post(req.title, req.content, '\n'.join(req.groups), req.image_paths)
    return {'ok': True, 'id': pid}

@app.get('/api/library/{pid}')
def get_library(pid: int):
    try:
        return STORE.get_library_post(pid)
    except KeyError:
        raise HTTPException(404, 'Không tìm thấy bài viết đã lưu.')

@app.delete('/api/library/{pid}')
def delete_library(pid: int):
    STORE.delete_library_post(pid); return {'ok': True}

@app.get('/api/draft')
def draft():
    return core.load_draft()

class DraftRequest(BaseModel):
    content: str = ''
    groups: list[str] = []
    image_paths: list[str] = []

@app.post('/api/draft')
def save_draft(req: DraftRequest):
    core.save_draft(req.content, '\n'.join(req.groups), req.image_paths); return {'ok': True}

@app.get('/api/stream')
def stream():
    return {'events': progress_events[-100:]}

# Worker that executes due schedules even while the web page is closed, as long as this Python server runs.
def scheduler_loop():
    while True:
        try:
            if not worker_lock.locked():
                now = datetime.now().astimezone()
                for item in STORE.list_schedules(300):
                    if item['status'] != 'scheduled': continue
                    try: due = datetime.fromisoformat(item['scheduled_at']) <= now
                    except Exception: due = False
                    if not due: continue
                    if not STORE.claim_schedule(item['id']): continue
                    try:
                        groups = core.clean_groups(item['groups_text'])
                        ids, duplicates = STORE.add_jobs(item['content'], item['images'], groups, repeat_key=item['id'])
                        STORE.update_schedule(item['id'], 'running', job_ids=ids)
                        launch_publish(ids, item['delay_seconds'])
                        # Wait until the publish worker finishes before marking the schedule.
                        def finish(sid=item['id']):
                            while publish_thread and publish_thread.is_alive(): time.sleep(2)
                            STORE.update_schedule(sid, 'completed')
                        threading.Thread(target=finish, daemon=True).start()
                    except Exception as exc:
                        STORE.update_schedule(item['id'], 'failed', error=str(exc))
                        log(f'Lịch {item["title"]} lỗi: {exc}', 'Lịch hẹn')
                    break
        except Exception as exc:
            log(f'Lỗi bộ kiểm tra lịch: {exc}', 'Lỗi')
        time.sleep(5)

@app.on_event('startup')
def startup():
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('server:app', host=os.getenv('POSTFLOW_HOST', '127.0.0.1'), port=int(os.getenv('POSTFLOW_PORT', '8000')), reload=False)
