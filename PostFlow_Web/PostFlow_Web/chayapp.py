from __future__ import annotations

import hashlib
import json
import csv
import zipfile
import os
import re
import sqlite3
import shutil
import uuid
import unicodedata
from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

HOME = Path(os.environ.get('FB_POSTER_HOME', str(Path.home() / 'FB_Auto_Post_V4')))
DB_PATH = HOME / 'cong_viec.sqlite3'
DRAFT_PATH = HOME / 'ban_nhap.json'
BROWSER_DIR = HOME / 'trinh_duyet'
EVIDENCE_DIR = HOME / 'bang_chung'
MAX_GROUPS = None  # Không giới hạn số nhóm; Facebook vẫn có thể áp dụng giới hạn riêng.
MAX_IMAGES = 20

QUEUED = 'QUEUED'
PREPARING = 'PREPARING'
SUBMITTING = 'SUBMITTING'
OBSERVED = 'OBSERVED'
PENDING = 'PENDING'
ACKNOWLEDGED = 'ACKNOWLEDGED'
REVIEW = 'REVIEW'
CONFIRMED = 'CONFIRMED'
FAILED = 'FAILED'  
SKIPPED = 'SKIPPED'
LABELS = {
    QUEUED: 'Chưa đăng', PREPARING: 'Đang chuẩn bị',
    SUBMITTING: 'Đang gửi', OBSERVED: 'Đã phát hiện nội dung bài trên nhóm',
    PENDING: 'Đã gửi · Chờ duyệt', ACKNOWLEDGED: 'Facebook báo đã nhận', REVIEW: 'Đã nhấn Đăng · Cần đối chiếu',
    CONFIRMED: 'Thành công', FAILED: 'Bỏ qua · Lỗi chưa gửi',
    SKIPPED: 'Đã bỏ qua',
}


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def parse_local_datetime(value: str) -> datetime:
    """Parse an ISO datetime or YYYY-MM-DD HH:MM as local Windows time."""
    raw = (value or '').strip()
    parsed = None
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S'):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        raise ValueError('Ngày giờ không hợp lệ. Hãy dùng YYYY-MM-DD HH:MM, ví dụ 2026-10-31 17:30.')
    local_now = datetime.now().astimezone()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=local_now.tzinfo)
    return parsed.astimezone()


def clean_group(url: str) -> str:
    value = url.strip()
    if not value:
        raise ValueError('Đường dẫn nhóm trống.')
    if not value.startswith(('http://', 'https://')):
        value = 'https://' + value
    try:
        parsed = urlsplit(value)
        hostname = (parsed.hostname or '').lower()
        if (parsed.scheme != 'https' or hostname not in {'facebook.com', 'www.facebook.com', 'm.facebook.com'}
                or parsed.username or parsed.password or parsed.port):
            raise ValueError('Chỉ chấp nhận https://www.facebook.com/groups/...')
        match = re.fullmatch(r'/groups/([a-zA-Z0-9_.-]+)/?', parsed.path)
        if not match:
            raise ValueError('Liên kết phải là liên kết gốc của nhóm, không phải bài viết.')
        slug = match.group(1)
        if slug.lower() in {'feed', 'discover', 'create', 'joins'}:
            raise ValueError('Đây không phải địa chỉ nhóm hợp lệ.')
    except (UnicodeError, ValueError) as e:
        raise ValueError(str(e)) from e
    return 'https://www.facebook.com/groups/' + slug.lower()


def clean_groups(text: str) -> list[str]:
    urls = []
    seen = set()
    for n, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        try:
            url = clean_group(raw)
        except ValueError as exc:
            raise ValueError(f'Dòng {n}: {exc}') from exc
        if url not in seen:
            urls.append(url)
            seen.add(url)
    if not urls:
        raise ValueError('Bạn chưa nhập nhóm Facebook.')
    return urls


def validate_images(images: list[str]) -> list[str]:
    if len(images) > MAX_IMAGES:
        raise ValueError(f'Tối đa {MAX_IMAGES} ảnh/bài.')
    result = []
    for image in images:
        p = Path(image).expanduser().resolve()
        if not p.is_file() or p.suffix.lower() not in {'.jpg', '.jpeg', '.png', '.webp'}:
            raise ValueError(f'Không tìm thấy ảnh JPG/PNG/WEBP: {image}')
        if p.stat().st_size > 500 * 1024 * 1024:
            raise ValueError(f'Ảnh lớn hơn 500 MB: {p.name}')
        result.append(str(p))
    return result


def norm_text(value: str) -> str:
    """Unicode-safe comparison: whitespace, zero-width and VS16 may change in editor."""
    value = unicodedata.normalize('NFC', value or '')
    return ''.join(ch for ch in value if not ch.isspace() and ch not in '\u200b\ufeff\ufe0f\ufe0e')


def text_is_complete(expected: str, actual: str) -> bool:
    normalized = norm_text(expected)
    return bool(normalized) and normalized in norm_text(actual)


def payload_digest(content: str, images: list[str]) -> str:
    """Contents + file *bytes* prevent duplicate posting even if image paths change."""
    h = hashlib.sha256()
    h.update(unicodedata.normalize('NFC', content).encode('utf-8'))
    for filename in images:
        h.update(b'\0IMAGE\0')
        with open(filename, 'rb') as f:
            for part in iter(lambda: f.read(1024 * 1024), b''):
                h.update(part)
    return h.hexdigest()


@dataclass(frozen=True)
class Job:
    id: int
    group_url: str
    content: str
    images: list[str]
    status: str
    note: str
    sent_intent_at: str | None
    created_at: str
    permalink: str
    evidence_path: str


class Store:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_url TEXT NOT NULL,
                    content TEXT NOT NULL,
                    images_json TEXT NOT NULL,
                    fingerprint TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    sent_intent_at TEXT,
                    permalink TEXT NOT NULL DEFAULT '',
                    evidence_path TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                CREATE TABLE IF NOT EXISTS saved_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    groups_text TEXT NOT NULL,
                    images_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduled_posts (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    groups_text TEXT NOT NULL,
                    images_json TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    delay_seconds INTEGER NOT NULL DEFAULT 15,
                    status TEXT NOT NULL DEFAULT 'scheduled',
                    job_ids_json TEXT NOT NULL DEFAULT '[]',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_scheduled_status_time ON scheduled_posts(status, scheduled_at);
                CREATE TABLE IF NOT EXISTS app_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT 'Hoạt động',
                    message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_log(created_at);
            ''')
            # A scheduled run interrupted by closing/crashing the app must never silently repost.
            db.execute("UPDATE scheduled_posts SET status='needs_review',error=?,updated_at=? WHERE status='running'",
                       ('Ứng dụng đóng khi lịch đang chạy. Kiểm tra Lịch sử Facebook trước khi tạo lịch mới để tránh đăng trùng.', now()))
            # PREPARING has not clicked yet (SUBMITTING is committed immediately before click).
            # Distinguish them on crash recovery to avoid false 'sent' or automatic retry.
            db.execute("UPDATE jobs SET status=?,note=?,updated_at=? WHERE status=?",
                       (FAILED, 'Ứng dụng đóng ở bước chuẩn bị, chưa nhấn Đăng.', now(), PREPARING))
            db.execute("UPDATE jobs SET status=?,note=?,updated_at=? WHERE status=?",
                       (REVIEW, 'Ứng dụng đóng trong lúc nhấn Đăng; có thể đã gửi, không thử lại.',
                        now(), SUBMITTING))

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=10000')
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def row_job(row) -> Job:
        return Job(id=row['id'], group_url=row['group_url'], content=row['content'],
                   images=json.loads(row['images_json']), status=row['status'],
                   note=row['note'], sent_intent_at=row['sent_intent_at'],
                   created_at=row['created_at'], permalink=row['permalink'],
                   evidence_path=row['evidence_path'])

    def add_activity(self, category: str, message: str) -> None:
        message = (message or '').strip()
        if not message:
            return
        with self._db() as db:
            db.execute('INSERT INTO activity_log(created_at,category,message) VALUES(?,?,?)',
                       (now(), (category or 'Hoạt động')[:60], message[:2000]))

    def list_activity(self, limit: int = 1000) -> list[dict]:
        with self._db() as db:
            rows = db.execute('SELECT id,created_at,category,message FROM activity_log '
                              'ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
        return [dict(row) for row in rows]

    def clear_activity(self) -> None:
        with self._db() as db:
            db.execute('DELETE FROM activity_log')

    def add_jobs(self, content: str, images: list[str], groups: list[str], *,
                 repeat_key: str | None = None) -> tuple[list[int], list[str]]:
        
        if not content.strip():
            raise ValueError('Bạn chưa nhập nội dung chữ.')
        images = validate_images(images)
        digest = payload_digest(content, images)
        created, duplicate = [], []
        with self._db() as db:
            for group in groups:
                seed = group + '\n' + digest
                if repeat_key:
                    seed += '\nREPEAT:' + repeat_key
                identity = hashlib.sha256(seed.encode()).hexdigest()
                cur = db.execute('''INSERT OR IGNORE INTO jobs (
                     group_url,content,images_json,fingerprint,status,created_at,updated_at)
                     VALUES (?,?,?,?,?,?,?)''',
                     (group, content, json.dumps(images, ensure_ascii=False), identity,
                      QUEUED, now(), now()))
                if cur.rowcount:
                    created.append(cur.lastrowid)
                elif repeat_key:
                    
                    row = db.execute('SELECT id FROM jobs WHERE fingerprint=?', (identity,)).fetchone()
                    if row is not None:
                        created.append(int(row['id']))
                else:
                    duplicate.append(group)
        return created, duplicate

    def latest_sent_intent_for_payload(self, content: str, images: list[str], exclude_id: int | None = None) -> str | None:
        images_json = json.dumps(images, ensure_ascii=False)
        with self._db() as db:
            if exclude_id is None:
                row = db.execute('''SELECT sent_intent_at FROM jobs
                    WHERE content=? AND images_json=? AND sent_intent_at IS NOT NULL
                    ORDER BY sent_intent_at DESC LIMIT 1''', (content, images_json)).fetchone()
            else:
                row = db.execute('''SELECT sent_intent_at FROM jobs
                    WHERE content=? AND images_json=? AND sent_intent_at IS NOT NULL AND id<>?
                    ORDER BY sent_intent_at DESC LIMIT 1''', (content, images_json, exclude_id)).fetchone()
        return row['sent_intent_at'] if row else None

    def get_app_state(self, key: str) -> str | None:
        with self._db() as db:
            row = db.execute('SELECT value FROM app_state WHERE key=?', (key,)).fetchone()
        return row['value'] if row else None

    def set_app_state(self, key: str, value: str) -> None:
        with self._db() as db:
            db.execute('''INSERT INTO app_state(key, value) VALUES(?, ?)
                          ON CONFLICT(key) DO UPDATE SET value=excluded.value''', (key, value))

    def get(self, job_id: int) -> Job:
        with self._db() as db:
            row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return self.row_job(row)

    def all(self, limit: int = 300) -> list[Job]:
        with self._db() as db:
            rows = db.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
        return [self.row_job(row) for row in rows]

    def transition(self, job_id: int, status: str, note: str = '', *, permalink: str | None = None,
                   evidence_path: str | None = None, submitted: bool = False) -> None:
        changes = ['status=?', 'note=?', 'updated_at=?']
        params: list[object] = [status, note, now()]
        if submitted:
            changes.append('sent_intent_at=?')
            params.append(now())
        if permalink is not None:
            changes.append('permalink=?')
            params.append(permalink)
        if evidence_path is not None:
            changes.append('evidence_path=?')
            params.append(evidence_path)
        params.append(job_id)
        with self._db() as db:
            db.execute(f"UPDATE jobs SET {','.join(changes)} WHERE id=?", params)

    def retry_before_submit(self, job_id: int) -> bool:
        """Only an explicit user action; no retry when click may have happened."""
        with self._db() as db:
            cur = db.execute('''UPDATE jobs SET status=?,note='',updated_at=?
                WHERE id=? AND status=? AND sent_intent_at IS NULL''',
                (QUEUED, now(), job_id, FAILED))
            return cur.rowcount == 1

    def confirm(self, job_id: int) -> None:
        with self._db() as db:
            db.execute('''UPDATE jobs SET status=?,note=?,updated_at=?
                WHERE id=? AND status IN (?,?,?,?)''',
                (CONFIRMED, 'Người dùng đã xem bài trên Facebook và xác nhận.', now(), job_id,
                 REVIEW, OBSERVED, PENDING, ACKNOWLEDGED))

    def skip(self, job_id: int) -> None:
        with self._db() as db:
            db.execute('UPDATE jobs SET status=?,note=?,updated_at=? WHERE id=? AND status=?',
                       (SKIPPED, 'Đã bỏ qua theo yêu cầu.', now(), job_id, QUEUED))

    def save_library_post(self, title: str, content: str, groups_text: str, images: list[str]) -> int:
        title = (title or '').strip()[:120]
        if not title:
            raise ValueError('Hãy nhập tên để dễ tìm lại bài.')
        if not (content or '').strip():
            raise ValueError('Nội dung bài đang trống.')
        clean_groups(groups_text)
        images = validate_images(images)
        created = now()
        with self._db() as db:
            cur = db.execute('INSERT INTO saved_posts(title,content,groups_text,images_json,created_at,updated_at) VALUES(?,?,?,?,?,?)',
                             (title, content, groups_text, json.dumps([], ensure_ascii=False), created, created))
            post_id = int(cur.lastrowid)
        copied = []
        folder = self.path.parent / 'thu_vien_anh' / str(post_id)
        try:
            if images:
                folder.mkdir(parents=True, exist_ok=True)
                for i, src in enumerate(images, 1):
                    source = Path(src)
                    dest = folder / f'{i:02d}{source.suffix.lower()}'
                    shutil.copy2(source, dest)
                    copied.append(str(dest.resolve()))
            with self._db() as db:
                db.execute('UPDATE saved_posts SET images_json=?,updated_at=? WHERE id=?',
                           (json.dumps(copied, ensure_ascii=False), now(), post_id))
        except Exception:
            with self._db() as db:
                db.execute('DELETE FROM saved_posts WHERE id=?', (post_id,))
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
            raise
        return post_id

    def list_library_posts(self, limit: int = 300) -> list[dict]:
        with self._db() as db:
            rows = db.execute('SELECT * FROM saved_posts ORDER BY updated_at DESC,id DESC LIMIT ?', (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item['images'] = json.loads(item.pop('images_json') or '[]')
            item['image_count'] = len(item['images'])
            result.append(item)
        return result

    def get_library_post(self, post_id: int) -> dict:
        with self._db() as db:
            row = db.execute('SELECT * FROM saved_posts WHERE id=?', (post_id,)).fetchone()
        if row is None:
            raise KeyError(post_id)
        item = dict(row)
        item['images'] = json.loads(item.pop('images_json') or '[]')
        return item

    def delete_library_post(self, post_id: int) -> None:
        with self._db() as db:
            row = db.execute('SELECT id FROM saved_posts WHERE id=?', (post_id,)).fetchone()
            if row is None:
                return
            db.execute('DELETE FROM saved_posts WHERE id=?', (post_id,))
        folder = self.path.parent / 'thu_vien_anh' / str(post_id)
        if folder.exists():
            shutil.rmtree(folder, ignore_errors=True)

    def add_schedule(self, title: str, content: str, groups_text: str, images: list[str],
                     scheduled_at: str, delay_seconds: int = 15) -> str:
        title = (title or '').strip()[:120] or 'Bài đăng theo lịch'
        if not (content or '').strip():
            raise ValueError('Nội dung bài đang trống.')
        clean_groups(groups_text)
        images = validate_images(images)
        try:
            when = parse_local_datetime(scheduled_at)
        except (TypeError, ValueError) as exc:
            raise ValueError(str(exc) or 'Ngày giờ hẹn không hợp lệ. Dùng định dạng YYYY-MM-DD HH:MM.') from exc
        if when <= datetime.now().astimezone() + timedelta(seconds=10):
            raise ValueError('Thời gian hẹn phải ở tương lai ít nhất 10 giây.')
        schedule_id = uuid.uuid4().hex
        folder = self.path.parent / 'lich_hen_anh' / schedule_id
        copied = []
        try:
            if images:
                folder.mkdir(parents=True, exist_ok=True)
                for i, src in enumerate(images, 1):
                    source = Path(src)
                    dest = folder / f'{i:02d}{source.suffix.lower()}'
                    shutil.copy2(source, dest)
                    copied.append(str(dest.resolve()))
            stamp = now()
            with self._db() as db:
                db.execute('''INSERT INTO scheduled_posts
                    (id,title,content,groups_text,images_json,scheduled_at,delay_seconds,status,job_ids_json,error,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (schedule_id, title, content, groups_text, json.dumps(copied, ensure_ascii=False),
                     when.isoformat(timespec='seconds'), max(0, int(delay_seconds)), 'scheduled', '[]', '', stamp, stamp))
            return schedule_id
        except Exception:
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
            raise

    @staticmethod
    def _schedule_row(row) -> dict:
        item = dict(row)
        item['images'] = json.loads(item.pop('images_json') or '[]')
        item['job_ids'] = json.loads(item.pop('job_ids_json') or '[]')
        return item

    def list_schedules(self, limit: int = 300) -> list[dict]:
        with self._db() as db:
            rows = db.execute('SELECT * FROM scheduled_posts ORDER BY scheduled_at ASC,created_at DESC LIMIT ?', (limit,)).fetchall()
        return [self._schedule_row(row) for row in rows]

    def get_schedule(self, schedule_id: str) -> dict:
        with self._db() as db:
            row = db.execute('SELECT * FROM scheduled_posts WHERE id=?', (schedule_id,)).fetchone()
        if row is None:
            raise KeyError(schedule_id)
        return self._schedule_row(row)

    def claim_schedule(self, schedule_id: str) -> bool:
        with self._db() as db:
            cur = db.execute("UPDATE scheduled_posts SET status='running',error='',updated_at=? WHERE id=? AND status='scheduled'",
                             (now(), schedule_id))
            return cur.rowcount == 1

    def update_schedule(self, schedule_id: str, status: str | None = None, *,
                        job_ids: list[int] | None = None, error: str | None = None) -> None:
        changes, params = [], []
        if status is not None:
            changes.append('status=?'); params.append(status)
        if job_ids is not None:
            changes.append('job_ids_json=?'); params.append(json.dumps(job_ids))
        if error is not None:
            changes.append('error=?'); params.append(error)
        changes.append('updated_at=?'); params.append(now())
        params.append(schedule_id)
        with self._db() as db:
            db.execute(f"UPDATE scheduled_posts SET {','.join(changes)} WHERE id=?", params)

    def cancel_schedule(self, schedule_id: str) -> bool:
        with self._db() as db:
            cur = db.execute("UPDATE scheduled_posts SET status='cancelled',updated_at=? WHERE id=? AND status='scheduled'",
                             (now(), schedule_id))
            return cur.rowcount == 1

    def reschedule(self, schedule_id: str, scheduled_at: str) -> bool:
        try:
            when = parse_local_datetime(scheduled_at)
        except (TypeError, ValueError) as exc:
            raise ValueError(str(exc) or 'Ngày giờ không hợp lệ. Dùng YYYY-MM-DD HH:MM.') from exc
        if when <= datetime.now().astimezone() + timedelta(seconds=10):
            raise ValueError('Thời gian hẹn phải ở tương lai ít nhất 10 giây.')
        with self._db() as db:
            cur = db.execute("UPDATE scheduled_posts SET scheduled_at=?,updated_at=? WHERE id=? AND status='scheduled'",
                             (when.isoformat(timespec='seconds'), now(), schedule_id))
            return cur.rowcount == 1


def save_draft(text: str, groups: str, images: list[str]):
    DRAFT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = DRAFT_PATH.with_suffix('.tmp')
    tmp.write_text(json.dumps({'content': text, 'groups': groups, 'images': images},
                              ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(DRAFT_PATH)


def load_draft() -> dict:
    """Prefer V4 data; one-time read-only fallback for previous V3 / V2.5 drafts."""
    blank = {'content': '', 'groups': '', 'images': []}
    if DRAFT_PATH.is_file():
        try:
            data = json.loads(DRAFT_PATH.read_text(encoding='utf-8'))
            return {key: data.get(key, blank[key]) for key in blank}
        except (OSError, ValueError, TypeError):
            return blank
    candidates = [
        (Path(os.environ.get('LOCALAPPDATA') or Path.home()) / 'FBPostDeDung' / 'du_lieu.json', 'v3'),
        (Path.home() / 'FacebookPostAssistant' / 'ban_nhap_v23.json', 'v2'),
    ]
    for path, source in candidates:
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            if source == 'v3':
                result = {'content': data.get('message', ''),
                          'groups': data.get('groups_text', ''),
                          'images': data.get('images', [])}
            else:
                config = path.parent / 'cau_hinh_v2.json'
                try:
                    settings = json.loads(config.read_text(encoding='utf-8'))
                except (OSError, ValueError):
                    settings = {}
                result = {'content': data.get('message', ''),
                          'groups': settings.get('groups', ''),
                          'images': data.get('images', [])}
            if not isinstance(result['images'], list):
                result['images'] = []
            if result['content'] or result['groups']:
                return result
        except (OSError, ValueError, TypeError):
            pass
    return blank

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

Progress = Callable[[str], None]

COMPOSER_RE = re.compile(
    r'(?:Viết\s+gì\s*(?:đó|đi)?|Bạn\s+(?:viết\s+gì\s*(?:đi|đó)?|đang\s+nghĩ\s+gì|muốn\s+chia\s+sẻ\s+gì)|'
    r'Tạo\s+(?:một\s+)?bài\s+viết|Viết\s+(?:một\s+)?bài(?:\s+viết)?|Chia\s+sẻ\s+gì\s*(?:đó)?|'
    r'Bắt\s+đầu\s+(?:một\s+)?(?:cuộc\s+)?thảo\s+luận|'
    r'Write\s+something|What[’\\\'`]?s\s+on\s+your\s+mind|'
    r'Create\s+(?:a\s+)?post|Write\s+(?:a\s+)?post|Share\s+something|Start\s+(?:a\s+)?discussion)',
    re.I,
)
PHOTO_RE = re.compile(r'^(?:Ảnh/video|Ảnh|Photo/video|Add photos/videos|Photo|Photos/Videos)$', re.I)
POST_RE = re.compile(r'^\s*(?:Đăng(?: bài)?|Post(?: now)?|Publish(?: post)?)\s*$', re.I)
PENDING_RE = re.compile(r'(đang chờ (phê )?duyệt|chờ quản trị viên|pending approval|pending admin approval)', re.I)
BUSY_RE = re.compile(r'(đang tải (ảnh|lên)|uploading|processing media)', re.I)
SUCCESS_RE = re.compile(r'(bài viết của bạn đã được (đăng|chia sẻ)|đã chia sẻ bài viết|\byour post (?:was|has been) (?:shared|posted|published)\b|post published successfully)', re.I)
NO_POST_RIGHTS_RE = re.compile(
    r'(?:chỉ\s+(?:quản\s+trị\s+viên|người\s+kiểm\s+duyệt).{0,45}(?:đăng|viết)|'
    r'bạn\s+không\s+(?:thể|được)\s+(?:đăng|viết)|'
    r'only\s+admins\s+can\s+post|you\s+can[’\\\'`]?t\s+post)', re.I
)


class PublishError(RuntimeError):
    pass


def first_visible(locators):
    for locator in locators:
        try:
            for index in range(min(locator.count(), 30)):
                obj = locator.nth(index)
                if obj.is_visible():
                    return obj
        except Exception:
            continue
    return None


def editor_value(editor) -> str:
    for fn in ('inner_text', 'text_content', 'input_value'):
        try:
            value = getattr(editor, fn)(timeout=1500)
            if value:
                return value
        except Exception:
            pass
    return ''


def _editor_is_post_input(editor) -> bool:
    """Không nhầm ô bình luận với ô soạn bài nhóm đang hiển thị."""
    try:
        clues = ' '.join((editor.get_attribute(a, timeout=750) or '') for a in
                         ('aria-label', 'data-placeholder', 'placeholder', 'data-testid'))
        return bool(COMPOSER_RE.search(clues)) and not re.search(r'bình luận|comment', clues, re.I)
    except Exception:
        return False


def _safe_inline_scope(editor):
    """Chỉ cho phép nút Đăng trong vùng chứa chính ô soạn bài (không phải nút ở feed)."""
    for selector in ('xpath=ancestor::form[1]',
                     'xpath=ancestor::*[@role="form"][1]',
                     'xpath=ancestor::*[@data-pagelet="Composer"][1]'):
        try:
            loc = editor.locator(selector)
            if loc.count() and loc.first.is_visible():
                return loc.first
        except Exception:
            pass
    
    for depth in (2, 3, 4):
        try:
            parent = editor.locator(f'xpath=ancestor::div[{depth}]')
            if parent.count() and parent.get_by_role('button', name=POST_RE).count():
                return parent
        except Exception:
            pass
    return None


def visible_editor(page):
    try:
        dialogs = page.locator('[role="dialog"], [aria-modal="true"]')
        for i in reversed(range(min(dialogs.count(), 8))):
            dlg = dialogs.nth(i)
            if dlg.is_visible():
                editor = first_visible([dlg.locator('[contenteditable="true"][role="textbox"]'),
                                        dlg.locator('[contenteditable="true"]')])
                if editor:
                    return dlg, editor
    except Exception:
        pass
    
    for selector in ('main [contenteditable="true"]',
                     '[role="main"] [contenteditable="true"]',
                     '[contenteditable="true"][role="textbox"]'):
        try:
            boxes = page.locator(selector)
            for i in range(min(boxes.count(), 25)):
                box = boxes.nth(i)
                if box.is_visible() and _editor_is_post_input(box):
                    return (_safe_inline_scope(box) or page), box
        except Exception:
            continue
    return None, None


def _composer_candidates(page):
    """Ưu tiên các nút trong main; dùng nhãn/placeholder trước, không click bài trong feed."""
    roots = []
    for css in ('[role="main"]', 'main'):
        try:
            loc = page.locator(css)
            if loc.count():
                roots.append(loc.first)
                break
        except Exception:
            pass
    roots.append(page)
    for root in roots:
        yield root.get_by_role('button', name=COMPOSER_RE)
        yield root.locator('[role="button"]').filter(has_text=COMPOSER_RE)
        # Có giao diện chỉ dùng div hoặc span thay vì role=button.
        yield root.get_by_text(COMPOSER_RE)
        yield root.locator('[aria-label]').filter(has_text=COMPOSER_RE)


def _wait_for_editor(page, seconds: float):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        scope, editor = visible_editor(page)
        if editor is not None:
            return scope, editor
        page.wait_for_timeout(350)
    return None, None


def composer_diagnostics(page, folder: Path, log: Progress):
    """Ghi nhãn bấm hiện tại để lần sau tìm được selector đúng mà không đoán."""
    try:
        data = page.evaluate("""() => {
          const visible = el => { const r = el.getBoundingClientRect();
            const st = getComputedStyle(el); return r.width > 0 && r.height > 0 &&
              st.display !== 'none' && st.visibility !== 'hidden'; };
          return [...document.querySelectorAll('button,[role="button"],[contenteditable="true"],[aria-label]')]
            .filter(visible).slice(0, 200).map(el => ({
              tag:el.tagName, role:el.getAttribute('role'),
              aria:el.getAttribute('aria-label'), placeholder:el.getAttribute('data-placeholder'),
              text:(el.innerText||'').trim().slice(0,100)
            }));
        }""")
        path = folder / 'chi_tiet_o_soan_bai.txt'
        path.write_text('URL: ' + page.url + '\n' + json.dumps(data, ensure_ascii=False, indent=2),
                        encoding='utf-8')
        log('Đã lưu thông tin các nút Facebook: ' + str(path))
    except Exception as exc:
        log('Không ghi được thông tin nút: ' + type(exc).__name__)


def open_composer(page, log: Progress, evidence_folder: Path | None = None):
    """Mở composer tối đa một lần; không click lặp khiến hộp thoại bật/tắt."""
    scope, editor = visible_editor(page)
    # Inline composer có thể không tìm được vùng chứa an toàn và trả scope=page.
    # Nếu đã thấy editor thì tuyệt đối không click nút mở composer lần nữa.
    if editor is not None:
        return scope, editor

    log('Đang tìm ô soạn bài trong nhóm (nhiều kiểu giao diện)...')
    clicked = False
    for locator in _composer_candidates(page):
        try:
            count = min(locator.count(), 18)
        except Exception:
            continue
        for i in range(count):
            node = locator.nth(i)
            try:
                if not node.is_visible() or node.get_attribute('aria-disabled') == 'true':
                    continue
                node.click(timeout=2500)
                clicked = True
                log('Đã bấm nút mở soạn bài một lần; đang chờ Facebook hiển thị khung đăng...')
                break
            except Exception:
                continue
        if clicked:
            break

    if clicked:
        # Facebook có thể dựng dialog chậm; chờ trên cùng một lần mở.
        scope, editor = _wait_for_editor(page, 8.0)
        if editor is not None:
            log('Đã mở được ô soạn bài Facebook.')
            return scope, editor
        # Chờ thêm một nhịp nhưng không click lại: click lần hai có thể đóng dialog.
        page.wait_for_timeout(1800)
        scope, editor = visible_editor(page)
        if editor is not None:
            log('Đã nhận diện ô soạn bài Facebook.')
            return scope, editor
    else:
        # Không tìm thấy nút có thể bấm; chỉ chờ giao diện tự tải, không thao tác mù.
        scope, editor = _wait_for_editor(page, 3.0)
        if editor is not None:
            return scope, editor

    if evidence_folder is not None:
        composer_diagnostics(page, evidence_folder, log)
    try:
        body = page.locator('body').inner_text(timeout=2200)[:20000]
    except Exception:
        body = ''
    if NO_POST_RIGHTS_RE.search(body):
        raise PublishError('Facebook thông báo tài khoản không có quyền tạo bài viết tại nhóm này.')
    if clicked:
        raise PublishError('Đã bấm mở khung soạn bài một lần nhưng Facebook không hiển thị ô nhập. '
                           'Đã dừng, không bấm lặp để tránh tự đóng khung đăng; hãy kiểm tra ảnh lỗi và '
                           'chi_tiet_o_soan_bai.txt.')
    raise PublishError('Không tìm thấy ô soạn bài trên nhóm. Hãy mở ảnh lỗi và chi_tiet_o_soan_bai.txt '
                       'để kiểm tra Facebook hiện nhãn gì; chưa gửi bài nào.')


def attach_images(page, scope, images: list[str], log: Progress):
    if not images:
        return
    log(f'Đang chọn {len(images)} ảnh...')
    file_inputs = scope.locator('input[type="file"]')
    if file_inputs.count() == 0:
        button = first_visible([
            scope.get_by_role('button', name=PHOTO_RE),
            scope.get_by_text(PHOTO_RE),
            scope.locator('[aria-label*="Ảnh/video"]'),
        ])
        if button is None:
            raise PublishError('Không tìm thấy chức năng Thêm ảnh/video của Facebook.')
        button.click(timeout=4500)
        page.wait_for_timeout(400)
    
        fresh_scope, _ = visible_editor(page)
        scope = fresh_scope or scope
        file_inputs = scope.locator('input[type="file"]')
        if file_inputs.count() == 0:
            file_inputs = page.locator('input[type="file"][accept*="image"]')
    if file_inputs.count() == 0:
        raise PublishError('Không xuất hiện ô chọn tệp ảnh của Facebook.')
    selected = False
    reason = ''
    for idx in reversed(range(min(file_inputs.count(), 12))):
        inp = file_inputs.nth(idx)
        try:
            accept = inp.get_attribute('accept', timeout=1000) or ''
            if accept and 'image' not in accept and '.jpg' not in accept and '.png' not in accept:
                continue
            
            inp.set_input_files(images, timeout=7000)
            selected = True
            
            log('Đã chọn tệp: ' + ', '.join(Path(p).name for p in images))
            break
        except Exception as exc:
            reason = str(exc).splitlines()[0][:180]
    if not selected:
        raise PublishError('Không thể chọn ảnh. ' + reason)
    
    log('Ảnh đã được chọn. Sẽ tiếp tục theo dõi trạng thái tải ảnh trước khi đăng.')


def fill_and_verify(page, content: str, log: Progress):
    """After attachments have been selected, find the new editor and enter text.
    Check *the current visible editor*, never a stale pre-upload locator.
    """
    scope, editor = visible_editor(page)
    if editor is None:
        raise PublishError('Không thấy ô soạn chữ sau khi chọn ảnh; chưa nhấn Đăng.')
    for attempt in range(2):
        try:
            editor.fill(content, timeout=5000)
        except Exception:
            editor.click(timeout=3000)
            page.keyboard.insert_text(content)
        page.wait_for_timeout(350)
        scope, new_editor = visible_editor(page)
        if new_editor is None:
            raise PublishError('Ô soạn bài biến mất sau khi tải ảnh.')
        if text_is_complete(content, editor_value(new_editor)):
            editor = new_editor
            break
        log(f'Facebook làm mất chữ ở lần nhập {attempt + 1}; đang khôi phục...')
        editor = new_editor
    else:
        raise PublishError('Không thể giữ nguyên chữ/emoji sau khi tải ảnh; hủy trước khi nhấn Đăng.')
    for _ in range(2):
        page.wait_for_timeout(350)
        scope, editor = visible_editor(page)
        if editor is None or not text_is_complete(content, editor_value(editor)):
            raise PublishError('Chữ không ổn định trong ô soạn; chưa nhấn Đăng để tránh bài chỉ có ảnh.')
    log('Đã kiểm tra chữ + emoji nhiều lần sau khi chọn ảnh.')
    return scope


def publishing_button(scope, page):
    candidates = [scope.get_by_role('button', name=POST_RE),
                  scope.locator('[role="button"][aria-label="Đăng"]'),
                  scope.locator('[role="button"][aria-label="Post"]')]
    if scope == page:
        
        return None
    for locator in candidates:
        try:
            for i in reversed(range(min(locator.count(), 12))):
                node = locator.nth(i)
                if (node.is_visible() and node.is_enabled()
                        and node.get_attribute('aria-disabled') != 'true'):
                    return node
        except Exception:
            pass
    return None


def wait_publish_ready(page, content: str, seconds: int, log: Progress):
    deadline = time.monotonic() + seconds
    last_note = ''
    while time.monotonic() < deadline:
        scope, editor = visible_editor(page)
        if editor is None or not text_is_complete(content, editor_value(editor)):
            raise PublishError('Nội dung chữ bị mất khi Facebook xử lý ảnh; chưa nhấn Đăng.')
        # Detect obvious media-processing labels in composer, wait rather than click.
        try:
            busy = scope.get_by_text(BUSY_RE).first
            if busy.count() and busy.is_visible():
                last_note = 'Facebook vẫn đang xử lý ảnh'
                page.wait_for_timeout(650)
                continue
        except Exception:
            pass
        button = publishing_button(scope, page)
        if button is not None:
            return button
        last_note = 'Nút Đăng chưa sẵn sàng (có thể do quyền đăng hoặc giao diện Facebook)'
        page.wait_for_timeout(650)
    raise PublishError(f'{last_note}. Hết {seconds} giây; chưa nhấn Đăng.')


def _find_published_permalink(page, content: str) -> str:
    """Bằng chứng yếu hơn xác nhận ảnh: chữ của bài + permalink cùng một article."""
    excerpt = norm_text(content)[:150]
    if len(excerpt) < 20:
        return ''
    articles = page.locator('[role="article"]')
    for index in range(min(articles.count(), 24)):
        try:
            article = articles.nth(index)
            if not article.is_visible() or excerpt not in norm_text(article.inner_text(timeout=1200)):
                continue
            hrefs = article.locator('a[href*="/posts/"], a[href*="/permalink/"]').evaluate_all(
                '(list) => list.map(a => a.href).filter(Boolean)')
            if hrefs:
                return hrefs[0]
        except Exception:
            continue
    return ''


def observe_publish(page, content: str, group: str, log: Progress) -> tuple[str, str, str]:
    """Wait for post composer to finish rather than navigating away too early.

    A click is *not* proof of publication. If the post may still be uploading,
    leave it as REVIEW instead of automatically resubmitting it.
    """
    deadline = time.monotonic() + 5.0
    ui_success = False
    composer_still_contains_post = False
    while time.monotonic() < deadline:
        try:
            alerts = page.locator('[role="alert"], [role="status"], [aria-live="polite"]')
            feedback = ' '.join(alerts.all_inner_texts(timeout=1000))
            if PENDING_RE.search(feedback):
                return PENDING, 'Facebook thông báo bài đang chờ duyệt.', ''
            if SUCCESS_RE.search(feedback):
                ui_success = True
        except Exception:
            pass
        try:
            dialogs_text = ' '.join(page.locator('[role="dialog"]').all_inner_texts(timeout=1000))
            if PENDING_RE.search(dialogs_text):
                return PENDING, 'Facebook thông báo bài đang chờ duyệt.', ''
        except Exception:
            pass
        if ui_success:
            break
        try:
            _, editor = visible_editor(page)
            composer_still_contains_post = bool(editor and text_is_complete(content, editor_value(editor)))
        except Exception:
            composer_still_contains_post = False
        if not composer_still_contains_post:
            break
        page.wait_for_timeout(300)

    if ui_success:
        # Acknowledged is deliberately NOT the same as confirmed with images.
        return ACKNOWLEDGED, 'Facebook phản hồi đã nhận bài; chưa đối chiếu đủ chữ và ảnh.', ''

    if composer_still_contains_post:
        # Do not reload and risk canceling an unfinished upload.
        return REVIEW, ('Đã nhấn Đăng nhưng ô soạn vẫn còn nội dung; có thể Facebook đang xử lý. '
                        'Không tự đăng lại nhóm này.'), ''

    # Only check feed after editor is gone. Try the current page first.
    try:
        permalink = _find_published_permalink(page, content)
        if permalink:
            return OBSERVED, 'Đã tìm thấy nội dung chữ và liên kết bài; số ảnh cần đối chiếu.', permalink
    except Exception:
        pass
    # Do not force a second navigation immediately after clicking Publish.
    # A navigation here can interrupt Facebook's post flow or hide a popup/tab issue.
    try:
        current_url = page.url
        if current_url == 'about:blank':
            log('CẢNH BÁO: tab Facebook hiện đang ở about:blank sau thao tác Đăng.')
        else:
            log('Chưa thấy permalink trên trang hiện tại; giữ nguyên trang để tránh gián đoạn xử lý của Facebook.')
    except Exception as exc:
        log('Không đọc được URL tab sau khi đăng: ' + type(exc).__name__)
    return REVIEW, ('Đã nhấn Đăng nhưng chưa thu được bằng chứng đủ để xác nhận bài. '
                    'KHÔNG tự đăng lại; nếu bạn nhìn thấy bài, dùng nút ĐÃ THẤY BÀI.'), ''


def screenshot(page, folder: Path, name: str, log: Progress) -> None:
    try:
        page.screenshot(path=str(folder / name), full_page=False, timeout=4000)
    except Exception as exc:
        log(f'Không chụp được {name}: {type(exc).__name__}')


def _attach_publish_browser_diagnostics(page, log: Progress) -> None:
    """Log tab creation/navigation/close events to diagnose unexpected about:blank tabs."""
    try:
        context = page.context
        seen = set()

        def attach(p):
            try:
                key = id(p)
                if key in seen:
                    return
                seen.add(key)
                log(f'[Trình duyệt] Tab được theo dõi: {p.url}')

                def navigated(frame):
                    try:
                        if frame == p.main_frame:
                            log(f'[Trình duyệt] Tab điều hướng: {p.url}')
                    except Exception:
                        pass

                p.on('framenavigated', navigated)
                p.on('close', lambda: log('[Trình duyệt] Một tab đã đóng.'))
                p.on('crash', lambda: log('[Trình duyệt] Chromium báo tab bị crash.'))
                p.on('pageerror', lambda exc: log('[Trình duyệt] JavaScript pageerror: ' + str(exc)[:220]))
            except Exception as exc:
                log('[Trình duyệt] Không gắn được theo dõi tab: ' + type(exc).__name__)

        for existing in context.pages:
            attach(existing)
        context.on('page', attach)
    except Exception as exc:
        log('[Trình duyệt] Không bật được theo dõi tab: ' + type(exc).__name__)


def _recover_facebook_tab(page, log: Progress):
    """If a click focused an about:blank popup, return to an existing Facebook tab."""
    try:
        context = page.context
        pages = [p for p in context.pages if not p.is_closed()]
        details = []
        facebook_pages = []
        for p in pages:
            try:
                url = p.url
            except Exception:
                url = '<URL không đọc được>'
            details.append(url)
            if 'facebook.com' in url.lower() and url != 'about:blank':
                facebook_pages.append(p)
        log('[Trình duyệt] Các tab sau khi nhấn Đăng: ' + ' | '.join(details[:8]))

        try:
            current_url = page.url
        except Exception:
            current_url = ''
        if current_url == 'about:blank' and facebook_pages:
            target = facebook_pages[-1]
            target.bring_to_front()
            log('[Trình duyệt] Phát hiện tab about:blank; quay lại tab Facebook đang mở: ' + target.url)
            return target
        if page.is_closed() and facebook_pages:
            target = facebook_pages[-1]
            target.bring_to_front()
            log('[Trình duyệt] Tab thao tác đã đóng; chuyển sang tab Facebook còn hoạt động.')
            return target
    except Exception as exc:
        log('[Trình duyệt] Không thể kiểm tra/khôi phục tab: ' + type(exc).__name__)
    return page


def _run_one_on_page(job: Job, store: Store, log: Progress, page, ready_timeout: int = 18) -> str:
    """Thao tác một nhóm trong phiên Chrome đã mở; không click lại sau lỗi mơ hồ."""
    folder = EVIDENCE_DIR / f'bai_{job.id}_{datetime.now():%Y%m%d_%H%M%S}'
    folder.mkdir(parents=True, exist_ok=True)
    store.transition(job.id, PREPARING, 'Đang mở nhóm...', evidence_path=str(folder))
    had_submit_intent = False
    try:
        _attach_publish_browser_diagnostics(page, log)
        page.set_default_timeout(4500)
        log('Đang mở nhóm: ' + job.group_url)
        page.goto(job.group_url, wait_until='domcontentloaded', timeout=15000)
        # No forced 1.2s sleep; open_composer handles late-rendering groups.
        if ('/login' in page.url or 'checkpoint' in page.url or
                (page.locator('input[name="email"]').count() and
                 page.locator('input[name="pass"]').count())):
            raise PublishError('Facebook chưa đăng nhập hoặc yêu cầu xác minh. Nhấn Đăng nhập trước.')
        scope, _ = open_composer(page, log, folder)
        attach_images(page, scope, job.images, log)
        fill_and_verify(page, job.content, log)
        screenshot(page, folder, '01_truoc_khi_dang.png', log)
        log('Kiểm tra nút Đăng và ảnh đã sẵn sàng...')
        button = wait_publish_ready(page, job.content, ready_timeout, log)
        scope, editor = visible_editor(page)
        if editor is None or not text_is_complete(job.content, editor_value(editor)):
            raise PublishError('Nội dung chữ biến mất ngay trước khi Đăng; đã chặn gửi bài chỉ có ảnh.')
        # Commit BEFORE clicking, so crashes never generate a blind retry.
        had_submit_intent = True
        store.transition(job.id, SUBMITTING,
                         'Đã ghi ý định nhấn Đăng; không tự thử lại nếu kết quả chưa rõ.', submitted=True)
        log('Nhấn Đăng một lần duy nhất...')
        button.click(timeout=6500)
        # Facebook may create/focus another tab during its post flow. Record and recover safely.
        page.wait_for_timeout(900)
        page = _recover_facebook_tab(page, log)
        status, note, permalink = observe_publish(page, job.content, job.group_url, log)
        screenshot(page, folder, '02_sau_khi_dang.png', log)
        (folder / 'ket_qua.txt').write_text(
            f'Nhom: {job.group_url}\nTrang thai: {status}\n{note}\nLien ket: {permalink}\n',
            encoding='utf-8')
        store.transition(job.id, status, note, permalink=permalink)
        log('Kết quả: ' + note)
        return status
    except Exception as exc:
        screenshot(page, folder, 'loi_hien_thi.png', log)
        detail = f'{type(exc).__name__}: {str(exc).splitlines()[0][:360]}'
        if had_submit_intent:
            store.transition(job.id, REVIEW, detail + ' Đã có thao tác Đăng; không thử lại nhóm này.')
            log('CHƯA XÁC MINH (không phải kết luận đăng thất bại): ' + detail)
            return REVIEW
        store.transition(job.id, FAILED, detail + ' Chưa bấm Đăng.')
        log('BỎ QUA TRƯỚC KHI GỬI: ' + detail)
        return FAILED


@contextmanager
def browser_batch_session():
    """Dùng MỘT Chromium cho cả lượt, thay vì tốn thời gian mở Chrome từng nhóm."""
    from playwright.sync_api import sync_playwright
    BROWSER_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            str(BROWSER_DIR), headless=False, locale='vi-VN',
            viewport={'width': 1290, 'height': 860},
            accept_downloads=False, timeout=16000)
        try:
            pages = [p for p in context.pages if not p.is_closed()]
            facebook_pages = [p for p in pages if 'facebook.com' in p.url.lower()]
            usable_pages = [p for p in pages if p.url != 'about:blank']
            if facebook_pages:
                page = facebook_pages[-1]
            elif usable_pages:
                page = usable_pages[0]
            elif pages:
                page = pages[0]
            else:
                page = context.new_page()
            yield page
        finally:
            context.close()


def run_one(job: Job, store: Store, log: Progress, ready_timeout: int = 18, *, page=None) -> str:
    """Chạy đơn lẻ hoặc tận dụng trình duyệt chung (tương thích code dùng run_one cũ)."""
    if page is not None:
        return _run_one_on_page(job, store, log, page, ready_timeout)
    with browser_batch_session() as session_page:
        return _run_one_on_page(job, store, log, session_page, ready_timeout)


# Những trạng thái cuối cùng: đều cho phép chuyển NHÓM KHÁC khi bật tự chạy.
# REVIEW = có khả năng đã nhấn Đăng; chỉ chuyển nhóm, KHÔNG thử lại nhóm này.
# FAILED = chắc chắn chưa nhấn Đăng; ghi nhận lỗi và bỏ qua, không tự thử lại.
FINAL_STATES = {OBSERVED, PENDING, ACKNOWLEDGED, CONFIRMED, REVIEW, FAILED, SKIPPED}


def can_advance_automatically(status: str, enabled: bool, allow_unverified: bool = False) -> bool:
    """Không nhầm 'được chuyển nhóm' với 'đã đăng thành công'.

    allow_unverified giữ trong chữ ký để tương thích bản V6 nhưng không cần bật nữa:
    REVIEW và FAILED được lưu lại, không tự thử lại, rồi chuyển sang nhóm KHÁC.
    """
    return bool(enabled and status in FINAL_STATES)


def describe_outcome(status: str) -> str:
    if status == OBSERVED:
        return 'Đã thấy bài có chữ và liên kết trên nhóm; chưa đối chiếu toàn bộ ảnh.'
    if status == PENDING:
        return 'Facebook báo đã nhận bài và đang chờ duyệt.'
    if status == ACKNOWLEDGED:
        return 'Facebook báo đã nhận bài; chưa chứng minh đã hiển thị công khai.'
    if status == REVIEW:
        return 'Đã có thao tác Đăng, cần đối chiếu kết quả; không tự gửi lại nhóm này.'
    if status == FAILED:
        return 'Lỗi trước khi nhấn Đăng; bỏ qua nhóm này, không thử lại tự động.'
    if status == SKIPPED:
        return 'Đã bỏ qua.'
    return 'Chưa có kết quả cuối cùng, cần kiểm tra.'


def login_browser(log: Progress):
    """Log in yourself in the visible Chromium profile; app never asks for passwords."""
    from playwright.sync_api import sync_playwright
    BROWSER_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            str(BROWSER_DIR), headless=False, locale='vi-VN',
            viewport={'width': 1250, 'height': 850}, timeout=17000)
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto('https://www.facebook.com/', wait_until='domcontentloaded', timeout=25000)
            log('Hãy đăng nhập Facebook trong Chrome, sau đó ĐÓNG cửa sổ Chrome để quay lại ứng dụng.')
            # User decides when to close. Maximum 10 minutes to avoid hanging forever.
            end = time.monotonic() + 600
            while time.monotonic() < end:
                try:
                    if not ctx.pages:
                        break
                except Exception:
                    break
                time.sleep(1)
        finally:
            ctx.close()
    log('Đã đóng trình duyệt đăng nhập. Phiên được lưu cục bộ trên máy này.')


# ===== GIAO DIỆN TKINTER =====

import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from tkinter.scrolledtext import ScrolledText

ROOT = Path(__file__).resolve().parent
ASSETS = ROOT / 'assets'
ICON_PNG_DATA = {
    'browser': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAEGklEQVR4nMWW0WtcVRDGf3PO2bu7d7eJpaSpmLaxaEUMYh6sbVFBBeuLCpYtQaEtPvmsCH2paVRQ8B9QEZGKWrMIxRdFEEGKhPpQxVqwxRppoNpQaJrdzebuPWd8uLuJaZNNU6R+cOGeyznz3Zn5ZuYIHYyOGsaOKIgCDL5w/p4knVaCCGuBUY1cn0x+su237IMKo0eEsbEAIAsf20Sb9068Qa7wBOofJKSCsjZCQTFOEfsjrea3F77YefjfHAJqQHSwcmJ7MOWjYqMdqh5NG4v/s2Yo4mJELOqTkybU9k9WHz4LKoZKVQBNxR2TqLQjtK4mmjZ85+DNPaBpw4fW1USi0o5U3DFAqVSz/AxUJl6zUe9YSGYSkGghOjfpoOqSVWKi3sgnM6NT1Z2vS1/ldLlA7XtxpeG2ZxbAGiFJwzWHu0Pa/uWcIEDIznpxsdW0fqpJ+VFnqW0BO5zlLCMT4EotZeP6HEYEY8AHRei8ZwTWQAig7TCiICJcqadoUAqRISg2s22HLbUtzphCSpj3IBYUEWgmyuH9W3jp2duJnFCb8/SWHEGVWsOzruQAmK2nlGOLNZJlT0FV+eH0LK+++weTF5sdUiB4YwqpIySZ9AWMZMZ3DfXw8r47+PPvJklLuXugyM+/1ylGhu2bi/w62QDgvsGYc1NzzNRTnDUEzWrokft7OPT8AAffOksxL1lFKEJIxC3JgUArVfrX51CFNz++wAfHL/LiM5v48Mu/yMeWkcf6OPrNJQD2P7mRz7+bpllPwQm0lMI6R+2r3QxuyiPS0ewi3DVrRNr5Ejj4VD9DgzE+wPuHthMUZhsp771yFwAz9ZRdQ9uwJpNzCJBzmeDm5sOyKr+OMBNDtvPerUW29ufJOYP3CgJWlorGL5ZeO4/dZb0sYeqzQ29/OsU7n02x8bYcLd/dUCcdvWXLhfGHcFaWLallCTsoRIae2FKO7cJPrEZYLtqu+7oSqmbFq3pt91h5/2r7zOpm/lvccsKuITUC1krWSVYJlQgEs6jwNROqQmM+MDPTwhm5IdEkacCrXlfsqxIayQwM3Rmz9/E+ekt2ofZWJATSoBTzBmHl0eYwkRLmFbIOU44tJ05f5dzUHAf29HNgT393phXw0deXMuLOrUFQTKQuhKazWAuZC9ZAreF57vAZnt69YTEnNzCMNShihF/O15k4M8u62OJDJ8DGhtB0yw7gThHPzvnru+8NIB8ZSgXTEdrSATxdHaoNVCaOG+OGFfUgVjWb2n29ubWzkTWLsOCZejHOBjg+XR2qCZVxS3WfH9g3ccrmNzwQmpeTbBjTvUetDg/qTWFD5Ocv/zQ1vnOYyrg1VCsKiNN0RJP6SZPricTFbTK5yQfExdbkeiJN6iedpiOAUK3o/3ERbuMWXfX/AXiYCdk5YpqDAAAAAElFTkSuQmCC',
    'check': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAEOElEQVR4nMWWX2hWZRzHP7/nOee8fzadutkmohSJ1rzSbI3IwAoiKA1qyxAlgm6cUJRXuXy3GXjRTUFGRCgVU9EoM7zLyIhJE9IbrWRX1oWyTaZu7/uef8+vi3cznXvnNqG+8Nw855zf5/n+zvec5xEmVCgYuroVQQHqv3p9lZRi1cQJs5B4RjXny/DW/X8CoAhdBaG72wFMFBOogBbv27KHrP80iXuUxAmiswKionhG8cwZyvHJwY7e925lCIWCobtb6z/ZstJ43pfGNy3qFA2Tf5czWylIxkOM4GLX75Jk2/D23osUCmJYfWGcLIdN3m9xpTjScpJOPDinAWg5SV0pjkzebxHkMKCsviAC0LBvy25Tk+nWsShCCOboq5rbSGqCwI2FhaGO3h5Z/HFbLRL8LFl/jYZJCth7qS8IOmGzolQyntVyfBaNnjSpDZZjZI2GCfcME8Gpw8htL99qmICRNakNlhtjTYIjnXNAxmVECJOIwPqUkggr5paVAI7UWJMYnUv0J8mK4UrxGm2rnuCHth6W1CzielScBFXRxN06Mzd5xnIjKrGteQOfPrOdhxct48e297m/rpFyGiOTWufdK+x6WGTZvAYOPPvmzfn+yxcZKY/iiYHbA8S0Do0IpkoTPDEU45D5mTxfPPcWimJE+PpiH5tPfEApiTDGTMJN49CKYTQuo6rUBlmc6m3XimlEzg84vqmTdU0rABgJx3j7p8/JeQEZ65Oqu9NENdjl0SFeW/0UXY+/ypXitZsBMGIoJSF5G3Dixd2sa1qBU8fV8ijPf9vDSFQka4MpYVM6NOPOOtZu5MMNb2DFUE4iCn2HaKpZQOQSaoIcxzftYm3jgzh1XAuLbDy2hzNXBqjPziNxabXGTd1Sp44H6horsDSms/UVEKGr7xAiwsmX9/BI4woUuB6VeGEc1pCdT+ySqrApgU4d84IcO0/tpxRHdLa2E6YxnY+1MxaVGYtD1i9tJkoTRsIxXvp+L2cuD9CQuztsGodKU34hhb6DCLCrtZ3YJexdvw2AxKUE1mPnqf388vfvLK1dROiSGf2sqqY0VUdjTR27Tx9Egc7WdlJ1WDF4xvLRb8f5ZuA0S2oXEs0QNi0QIHWO+/ILKk4F3m1p59KNQb4b+JV3Th2gPltLOk1AppI0fLZ1pcR6AYO94yuduAmhlIY8tHAZw+XrDJduEFhvqq2oWgFwpOpLs+dS51kjVWEAipL3Mvxx9S9845HxfFCdGaxSAAzWpc4zNo0u4fSsZDyAqv1xquT9DJ6xqM4YBZUNGJyetWl0yQzuODqqcAxrQKsDJ6CzQkGlpjUoHBvccXTUcKTNDnX09rhidM7UZQMguht4xiCITF02cMXo3FBHbw9H2qzhfLMCouhmV4z7Tc4PJOtVjhoyxwFI1rMm5weuGPcruhkQzjfr/3AQntB/dNT/BzC+F6fUMPcVAAAAAElFTkSuQmCC',
    'delete': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAEYUlEQVR4nL2Wz29UVRTHP+e++950mJlScEQChaVWJEJFUDHiwqiJAokmIyUIJv4HRhcVhTqFaFxIXKgbFYkCxU6iRFnqAhM3GAML5JcxhAIG0lJKyzCdN+/d4+K1/LAzLUXDNznJy8u795Pvueecd4UxaVeXoVhUAQW4Unj9gYFaWUNnhWkoMJHm/YzMLO08CaAgdHWJFIsOQMZfjoNOrFm/LW29ZyLV5VHsRNFpAQVR6xm1Ir9Vovjnth97ttzMEKXLCEU9vnbD/WlPvg6MWeFUKUcR0yLdJAUy1mJECJ07VIl104M/7DmldIkpFY4JoJ66fTnfXzES1sJyLYrHF95JAJRrUTwS1sKc76/w1O0DtFQ4JgJwYvX6rS2poHglDEOE4A6NNbIbzgyCYKgadrUd6OmWo4VC1o7aX7LWtpejKEbw/mdgnLHWuxpFh6OmaJWhGiw0Qns5jmgIk9s4TWOSmLAWrxxHGKGdarDQpI2JnBI33NIYNI4nBYtn0Moo7lqlLlQAp8RpYyITOic0Kn0RXLmMl8smsDieABVriQaHyDy2jOZVK3HDw/WdohI6J7aRMUTQKCJonUdrcTOVk3/y9/s78JpzCdQ5xPeJhq6QWbaU+e+8ifg+ds69DPZ+h5fNos5N2HZy4GiV7OPLCRbMJ1gwH62GXNjxKSabRQKf6PIQmfYltG7bjFgLIsl3tahh+hsDncPksgx++z0mlSL/2npaXnwORLjw8WfoSEzmkSW0dr+NSafRKEKMQcNw0iJrDByDes05+nftBRHymzpoeeFZtFpl+OCvtG5/F9OUIhq4hMnMuO5yMtU73VukzmFntdC/czcDe3oBmPXSahZ+tB3TlGL0r9Oc2/oBuLEZozrJblM5HIfGMTZ/Dxc/+Rwvl2PW6udBlejSIH1vbcGkUkjq9gbUlA5hrPQvD5F7aiXZJ5aDMYi1eDObyW9cl7ga79UpNKVDsTYp/faHaS12YtJpKsdPUf79CPlXX2H2y2sI5t6HC2t4qdR/A4oxRJcGyTy69Ho1Vs/0cb74IeG586BKfuM6sitXTDmNpgYaIa6Mknv6SeZ1voFJN1E9fYaznUXi4RH8uXPo/3J3At3UUXcKTdOhgCri2wR25ixnO98jHh5J+q4WYWe30P/VHoAEehuygTFai93EWnYOb0aa4Z8OotUatQsXiS4PJSNrLH0ax0nLfPENJp3GpJsY2LUXrzl3I8U3DGhgjMrRtRsWBbg/GnWPGEN8rYJYiwQ+1JmPiCQTZuxZfH9CPwoQYh4ypMI+pxzOeBaUCbWtzmFmpBHf1odBkvogSOLfMCXOeBanHCYV9pnFpdJVlP02+aXUbybnppwgqN6IWxVbY0DZv7hUump6CwWv7UBP93AYHsmnmwKUsJ7TaUuJUcJ8uikYDsMjbQd6unsLBc8USosUkFhMx0itdigX+EHGtx4keb+TAMj41ssFfjBSqx2KxXQAUigt0rt/Eb6egbt01f8H46kX6OUL4rEAAAAASUVORK5CYII=',
    'photo': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAEvklEQVR4nL2WX4hUVRzHP79zz9y5uzOra2tq5f/8w+oaCLooRob1UGFW0oQQCEVQUQRF9FLrOutLD4JPRT1FESIulJW9VQ+SEiukhShqaYhkYtruujM7c+ee8+vhzo6is+vuiz84XO695/y+v9/39+8Ident7TU7iztVEAXYUzi3fLR2RRMvwhTEGtWWzL3ydv/i0wCKys7enVIsFj1AXZkKdaC+pwd2hTb7mFe3NnGJSGPP5ERBbWDVSHA0Tqo/7viuu+dmDOlFTRHRvi0/L8sEbV9YE3Z7dcRJGUHQqaDVPVCU0LZiJCDx8UDNXd++49uHz/SiYk4W+gVQ0XBflMl1V+LhOK6VXZ2O1OYpLK2bGNfKrhIPx1Em1y0a7gP0ZKE/jc+uzb/saMm2F0fjoRiRsGGtpGomy+nYXh17AVCNW8Lp4Wh1sLfn4Lo+6S2cyNtK6VBoc6vjpOwQAhFQBe+mSugNQ40V1AOKC21rECelY0mUe8RSHZkvYldXXRmpgyU1RQxEeYt6ZbJ5mnooqCrl4YQwMigEVVfGSLCa6sh8mzVRkrjYiUgAiirY0LD13cUs7W4nsGbygAqqysi1Gl/vPsf534bIZA2o4NW7bBAlNvFxI/WNgfJ1x0ObOujccA/X/q5w/WoNE8DoiCMTGmzWpJrH81BgXmcbj780l0/eHCQTjRUckvhYbLOD3inq4chXl/jp84tMn5Wlc8MMLp0t88+5EpmsuQ1TBFyiRPmAD77pxiXNjWoKCCAGMtmAlmmWl3evYF5nHu+UvcUznB0YJMoFaVLcCpgLEGHcMJjxAAEqpYSFXW3M68xTLTtMIKx/dg4u1rSleEW9NihuvE8gEwKGUcDFMyUGL1fJtgYAnDh0FWNvhFGMkNSUeNRNKrnGpRTSJKqMOL7sOc3azbM5d2yIk4evkQlN/b9QGqrRtbGD+Svb+P6jv4jaJlQ5sYdjJTJ0pcreHaeZvaiVLW8twhghiT0KRLmAjS8+wIbn72PGnCy1UT+RyokBxQjeKeVhx9b3HmTT9rms3Tyb1z9eRfuskMHLFV54fyn3L8mhCk++ugDvdbyqAe5AabXsCLOGJ15bwPrn5jRSvWNuxCt7VnLqyH8sWdPeaIFdj3bw569DaTxN84A2BZR69GfOjXjj01XMWtgKQGBvKMm1Z1jz1Kzbzj7zzmLUK67WvCU291BAPSxfN4PSYI3zvw9jbjmtmlInRtI2Jek3I8L8lW1N1U7gYVr46pVq2WFDgx+nvtTVJ2AdUEVBwIbSNJbWmlATF6sA3qdZd/boEKcOX2NZ9wxmzmsZ19qmBqgy/G/MD59dTEOgjfGs1oRqq75iM2IDj0fq3iWxp//DP4hyFlWd4gC+ZTxpeg8wYoKqr1hLNn9BK6Vj2foAVghsJqWjMpJMybsxEYGwxTQGcLY+gInyF0yxv2sE1QOBsYCmd5k694EVjBWCSa6xvRLITY1dXZD2wgPF/q4RUyjsD3oOrusbjYeP51s6QlRjlBvAOjZY77y46YniUI3zLR3haDx8vOfgur5CYX9gVvQXFBCVeFulVhqIwmlhmEk7dZrwU1v1IiHMtAZROC2s1EoDKvE2QFb0F/TuX4THNt6tq/7/0fGBFsUFDhwAAAAASUVORK5CYII=',
    'refresh': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAFIUlEQVR4nMWWXWhcRRTHf2fm3ru72dttaxKrUut3U4OVVrRYq4JU1Ac/QKyoiA+KtDSKItg3bavoS1EUrIgKVkERFKooikULfiBS8QOj0Sr4UQu2NW3SZHezuffOHB/uJpsmm9q+6MC83DtnfnPOmfOfI0yMjWrYhCKiAEsfbPSk9UFVh3AcQywadnRJ/5bibgBUhU0Im8UDE5upQA5a0vfHoxJ2rFaXXqQ+FdHjA6qgYkIVG36paf2jn7ae9tBUhrBRDZtFe9b9uthE8SsSRCvwDp/WQAT0GD2bOLoCKCYsg7FoluzySfWO3c+d+TMbVQwDCKDY4HVbiFe4xkji05prWR/b9F5xXpHmCX1ac64xkthCvAIbvA4oA81zLen7/WFbmL85awwnIhIdzRNrQESaIMF7xSuEgWAExhLFTEmCqiZBcV7kxoc2/rT19Eekd/3+2EnjExvGy11acwJ2NpDzMDrmcb71vdIh7BtyPLeum0qH4c5n/qa7YibXKDgblq1Lq99YLV4eJG5sURjY5T6pITITJpJHdqjmKReEmy+N6ZpjUVWMEd77qsbKniI3riwTWOG8RSG/HcgohYJXELA+qWHELE/c2KIgCIJMcQ4xdvoNMQJpnk1uuqTMXasrnH/6kRG//7q5lKI8tMbA2qsr3PfiQcoFg3c6eWrFuyAIskAdIgaZfvlFIHWKILx4TzeX9RZnhLk+rhQjwXvQZj5vWFHm+Q9G+e1ARjECPxF+RdQjpl2+8mTnc9t9LdjBUcdT7xzmhsf3cf1j+9jZP4YRMKZ1mQIr3HXlHKoNj5GZJRy0gwVGOFh13HpZzMqeIqrw4Xd1Nmw7xIERhxUhLgq//JXy5NuHueCsiCULI+KiEBcNt14e89qnVb7fk9AR5bmcHSiQeSgXDGuvruAVRuueDS8fYqjm6IwtThVV2LJ9mMwplZIhDISFnQEXnl0A4KyTQn7cmzSF4ChAa4ShquO2S+ew+JQQgG07Rzkw7OisGNKstcMJsUFEcM1a/HV/yg97EhSolAxRIP8OFHIPT5zXqpCP+scIA8G5I9fmtaaTdoVQKEUy+W86rH1Im8bJFE/aJX/6MM16dW0gR6xr91GBcIoEdM/NlWM2rhFopHlY7az3fhag90pcMOz4doxqw6NA3zUVhBxopkFDKzRS5ZyTQ6yF4ZqfseboQIWOgtC/J2H7FzUEWHZmgTWryuwddCQu9yKvOxgccRQjYevaLt54cAFXLeugnuSyd0zASS+Lwgs7RsmcosDjt5/AE3d2csp8y3DNc7juyRysWRXz5oYFnLEgpPfUiFXnFhgb11m9lJ51e3vEuAHEmKlaak3+MlyxtMQL67sJg3yHkbqn/48EEZgfG85d2NLW7V/UeOClg5Si6eUgoN6rt72ypG/vYnADMFO8rYFDVc8V55W499oKFy+eqaeQS96rH1d5+t3DWAOBbQPEO7C9QZZlQRhYq6pMF3DnoTO2fPZjg893N1i9tMTdV1XoqhhUc5F4/+s623aO8udgxvw4z9CM+lNFxNg0y4JjeoCtYVLiCqFgbetktYanGOUFn7npljMfYDPw7IKqoG+JCUC1jUlLNeaWDVGYtxITc145l7B2sKZ3TkyAoG/lrDVqeUNcT9+eb8KOrmVpbTBpvvxtW43jGE4VF5a7orQ++O3urYuWs0atobfZDbnsFjde3WWLlciE5RwmQp7Y45hNOTJh2dpiJXLj1V247BZA6EX/h0Z4YvxHrf4/6wx8zANxx0oAAAAASUVORK5CYII=',
    'resume': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAD4UlEQVR4nL2W34tVVRTHP2vvc849c8aZOxNNapmlhEM6lD7kU799qocg4pJE9BAWEf4BCuWdmR7qDwilxBcfwrwQFmQQViTmwwhpP1AMihAzf1XeuZf745yz9+rhzIjYaPdO4dqva+/P/q619lpbmLNqtWompyZVEAXYWTk7fqV5WW0oQh/mMtWRJbfL67UVpwEUlcnqpExNTXmAucNUmAO989T3bwVhaZPX/KHc5SJXfXozBQ1soEaCY3nW/WLbZw+8eS1DqqiZQnT6me/WDGhpb2CjjV4dad5C+mNdA1WiIMGIJXfpTFu6L+345MGfqqiYk5WaABo62ReHQxs76WyaZi03v3ExCyDNWq6TzqZxOLQxdLIP0JOVWpGft58+sWMgKk+10noqItH1N57Pouoi1KqmSVSO2mm9uv3g+ml5t/LjktmWO1yyyYY0bzkEez0sTwtSGBu865OquChIbNe1jg8n9lFT76YrDWZD6losBMtS5Y5VMaN3lmjVc0RATB9AwaauhcFsqHfTlaZk4tyrdwsViBghbTvunhjklV1rWPfECGnHk3UUY3svKEHw6l3JxLlxPrtp6YsIeVcJY0Oluopnt93D8FhIu+HmLtUrFHE+681dDKDgMs/EplFe2z3Oky8vI+8WasX0/lp7z4YUIVYPUWJ55MVlbNk1ztLVMZ2mQ31vYe4n/QV3Tq16WHbfAFt2ruHhF5YiInSaDmOFmzXDvoEFtQCrL1Rv2rKcV98bZ+1jIzT/ysjSGz+dxQHnuXO7vVduu6vEc2/cy/PTqxld/o/e8f8Ar4JFcLkiBtY9PsKK+wdRz4KFFPxXmHdFsdhAOH20zqH3z3Hp1w4DQ3bBVrhooCqoFrCLv7T55sOLnDp8BZcpSTm4YQvsH6hFzopqFI59fJmv956ncSklKQfY8Ob9ti/gfPiMFf482+XzXb9x6kideNAyOBoWoH8ZKb0BtYAFkaHdcMx8dIlvP/2D+oWUJaNF+HqdIoE1oToyveFbVRALxgo/H5vlyz3nOfNDk6QckIwEuLw3kIJaE2rQ9Z3ASmgVv6CXsdBpOo58cIGv9vwOBobHQlwfqhTFiLFd3wlMuRSd8fjjkU1Acdc6eq+UBi2nj85yaPc5wsQQDZhCVa9zWHGRTfD44+VSdMZsrU00RfWANQGKuoX2iEAyHFztof2Yos6aAFE9sLU20TSVyn67/eD66XbWODEUj0WoptcrBRb1tUA1HYrHonbWOLH94PrpSmW/NWtrFQUks7q5kzVm4mg4isLEQjGpF7MAojCxcTQcdbLGTGZ1MyBraxW99R/hecdb9dX/G6YQDTfY/Ij4AAAAAElFTkSuQmCC',
    'send': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAAE4ElEQVR4nMWWXWhcRRTHf2fm3rt7k02iTUNq6kfV1mIr2oBNfRKl4EcfFKsVnwQVifiBvimiXVPf9EEKWgmKnwgSFWpFxE9UKtpKTak0talVqZKmtrZbm83u3Xtnjg+7CbZ2K6ugf5i3Oed3zsycM0eYUbFoeGxIERSgb+T+xZTKigmEVuQz5bR2mbh5/W4AFOGxojA05AFmnAnUQWc8N/g4oV2J1+VkXhBtDaiiBEYx8jWp+3j/ncOP/pkhFIuGoSHt23D7BbTnX5EgGFDn0SQFmY2jBQmoIrkQsQbNsq2Uq7dO3P3COMWiGJaOCaA+DF+XfG7Al5OaVlNXj1brvJZWPUCtps6Xk5rkcwM+DF8HlKVjIgDzhgfX2o7ckJ9KaggRgBFp2Lea4QlSaqaQi9yxpDg5OLxOep6+u2Cj7HMTR/2NzKyIMJ0mBMYSBxHOe7Tlo52Vk3xofaU26mrB5cbG7mwx0q/VGoA1IlSzGivmLWJ+YQ6T0yUydQTGYKS199OQ1WoNMdJvY3e2MUGQ4XE0nAlC6h1duTY2rxnikeU30BHGHKoco5wmWDEYMa0hRcDjTBBkhtQd9/SderqiNt7e+zVb9n/P2hU38emNa3nw0utZ1rOA39MKU2mldbCokLrmFnkb8sS2TTj1nNnRTXHFTXyy+lFeveoeBnoXUkrK/wh80p1OPR1RzBcTu/lo37cA1HwGIlx33qW8f8PDvHbNfazoXcSRpMxUrQ62Yvi7Wz5laNYYnhp9l8w7VCHzjsRloMyCR1Y9wGVnLKKUlDmSlHGqBMY2BQfNYE49nbmYzRO7+eTnnVx1zsUn3bdqQT+rFvTz5f5x3vnxG17Z9Tm/Th+lI8oTmr+6l77n71qs6scQMSeWmmnU47Kec1m9cDlOFSPCyrMuoj3Mk3lHIczT29Y1a7P36AFe3vUZm/Zu48B0CRFpdFH1ImZJ0wwBvCr5IGK8tJ9ndnwACooSiKE77qCapSw+vY+5cQfOeyIbcH5XL3csvZI9RyaZKB8mlOC4pnFKYGAMByu/UxxYw0PLrz/VVqw1jB3+hfWj7/HW91tIG9mf2KGaAgUhcRnzC93ctvSKuqECIqQ+IxCLEcGpZ2T8S17a9Rk7f/uZw9UpTsu111ui+r8m0TRiMRyuTnH/smvpbesi9RlGDBYhMgE1n/Hm+Bae3fEB3/z6I4GxtIc55sYdZN6fFNYUKAKJS5lfmMOdF60k8372xSUu5Y09X/Hsjg8ZPfgT+SCkOy7gFVQ9mT856JRAK5ZDyRT3XHI1PXEnAHtKk2ye+I4Xxz5l24EfyNmQuXEBr/q3kOOBoVUS1ZlKFYRKlrLw9Hk8MrCavaUDbNjxPiN7vuJQ9RhtQcScfAGlNRAqSmQ18FkWiMHWf2rBiFDJEi7vu5Ant21i/fb3KCXTdEUxPXEnXpvfT3OYghHrsywIXMXus1E2OvMBO/W2M4rZ+MNWjiYVOqO4UWeOzLvWQHU5yUczH/A+c/DeDVOobBRrQJn1KAjd+QJWTL2X/hMUgOLqvmXjwXs3TBlG1tjJweF1rpxsN11tEUptBuz0X4wWikOpma62yJWT7ZODw+sYWWMNO5coICZNb9FqstW05yLJh7aeZqMPtrQak0M+tKY9F2k12WrS9BZA2LlE/4dBeEb/0aj/B4EJd9NYcugxAAAAAElFTkSuQmCC',
    'stop': 'iVBORw0KGgoAAAANSUhEUgAAABwAAAAcCAYAAAByDd+UAAADGElEQVR4nO2WwWtcRRzHP79582Y3u7YpWCxW7SGChaJgBONND4J6KnhYbCn04EUQvHqry6b/h4JQE5JL0IsGPNRjPLSngkKDLFvUi7JJdtOd92Z+Ht5uTdn3skksPfmF32Xeb36f3/fNvDcjjKXttqHTUQEFeHj544sPskwlRuEYUmP0hTSV+rdf/gKgILTbIp1OBJDJ4AR0/4NrNxsmeTfT+GYeoqjosYCiojYxmor5eRjDjy9/f+vGQYYobSN0dPv9a6/UjPm6lpiloMogzzkW6aBLoGktiQijELdGMV5f+OHWr0pbzDr3ZJyzesqmSzs+84MsD5OJJwmAQZaHHZ/5UzZdAlYBXeeeCMD2e1e/mHe1Tt97j+BOaKzKrp93zvX9qL2wubIsf77TemZQS39qWrs4yPOAkDxhYGhamwzy/E5zlL1t+3PuQoouDkJOKUwAMRxpQVUh6uNjQjIIOWJY7M+5C7YhJh/FEAwkWlYkKnG4VxSbIXEOqbmpXCnKhIYxufUxCqhMWxDQCCKc+egyc2+8hqKImGlXQOjv0l/7Dt/tIW4aCio+RrGV7Roh7u5z5uqHPPvp9ZnuAOqvXqT3yecQikZLy1bOlqL7xluvQ4zg86JQWcQi0pfO4148j458JbDa4Vg68mAMJEBS3V+RrGieV8IOdzjRIZNLc2fkzwY+Yf0PPNIHf5zcmUCxttj2R4GJFDv6vwBDf2dmkUewEInD4aH51U+iIs7x9zcb+Pu/zfgGhTjc56+vVsl6f5T+TyeyzhgdxZKnqkjNkXV7PPjsBunzz/3rpMSdPhzhe79jGnMVSyDqjFE71GhTISltSAuXhIDf7o5/d1PmijFjMM1GKUwBIyRDjdbO7/vuoJbeqTyAVcEIUq+VvqLHVObswAE8P/Jdc+72+h7ohi0WOpQW0jF4VpQrFLV149zt9T2zRitZ2FxZ7mfZ3bO1ukPxaAX4OFICij9bq7t+lt1d2FxZXqOVmBaXlGIlruzm2dZpl7pmahPGgycJgGZqk9Mudbt5tgVcAaTFJX36F+FHb+ApXfX/AX/IvAC7p9vRAAAAAElFTkSuQmCC',
}

COLORS = {
    'bg': '#f2f5fa', 'surface': '#ffffff', 'ink': '#19253b', 'muted': '#68768b',
    'primary': '#1d55c9', 'green': '#148a61', 'orange': '#d98712',
    'red': '#cf4356', 'purple': '#784ac9', 'line': '#dce4ef',
}


def wait_before_next(stop_event, seconds: int, log: Progress, step: int = 10) -> bool:
    """Đợi không khóa giao diện; có đếm ngược và cho phép nút Dừng hoạt động."""
    remaining = max(0, int(seconds))
    while remaining > 0:
        part = min(step, remaining)
        if stop_event.wait(part):
            return True
        remaining -= part
        if remaining:
            log(f'Đang đợi chuyển nhóm: còn {remaining} giây...')
    return stop_event.is_set()


class PosterApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title('PostFlow • Facebook Publisher | Quản lý đăng bài')
        # Kich thuoc linh hoat theo man hinh; noi dung tab co the cuon doc.
        self.root.geometry('1440x900')
        self.root.minsize(1100, 700)
        self.root.configure(bg=COLORS['bg'])
        self.store = Store()
        self._theme_mode = self.store.get_app_state('theme') or 'Sáng'
        self.images: list[str] = []
        self.selected_image_indices: set[int] = set()
        self.ui_events: queue.Queue[tuple[str, object]] = queue.Queue()
        self.worker_running = False
        self.stop_event = threading.Event()
        self.photos = {}
        self.current_ids: list[int] = []
        self.repeat_key_next: str | None = None
        self.schedule_check_busy = False
        self.autocontinue = tk.BooleanVar(value=True)  # Tiếp tục nhóm khác cả khi lỗi/không rõ
        self.delay_seconds = tk.StringVar(value='1800')
        self.status_text = tk.StringVar(value='Sẵn sàng. Đăng nhập Facebook trước khi chạy.')
        self.summary_text = tk.StringVar(value='Chưa có kết quả')
        self._save_after = None
        self._setup_styles()
        self._build()
        if self._theme_mode == 'Tối':
            self.root.after(120, lambda: self._apply_theme('Tối'))
        self._restore_draft()
        self.refresh_jobs()
        self.refresh_library()
        self.refresh_schedules()
        self.root.after(180, self._handle_ui)
        self.root.after(1000, self._check_due_schedules)
        self.root.protocol('WM_DELETE_WINDOW', self._close)
        self.root.bind('<Control-f>', lambda _e: self.open_global_search())
        self.root.bind('<Control-F>', lambda _e: self.open_global_search())
        self.root.bind('<Control-Shift-S>', lambda _e: self.backup_data())

    def _navigate(self, page, label=''):
        """Navigate from the persistent sidebar and highlight the selected item."""
        self.tabs.select(page)
        for button, nav_label in getattr(self, 'nav_buttons', []):
            active = (nav_label == label)
            button.configure(bg='#282B64' if active else '#101D3A',
                             fg='white' if active else '#DCE5FA',
                             font=('Segoe UI', 10, 'bold' if active else 'normal'))

    def _setup_styles(self):
        style = ttk.Style(self.root)
        try:
            style.theme_use('clam')
        except tk.TclError:
            pass
        style.configure('Jobs.Treeview', rowheight=31, borderwidth=0, font=('Segoe UI', 9),
                        background='#ffffff', foreground=COLORS['ink'], fieldbackground='#ffffff')
        style.configure('Jobs.Treeview.Heading', font=('Segoe UI', 9, 'bold'),
                        background='#e8eef7', foreground=COLORS['ink'], padding=9)
        style.map('Jobs.Treeview', background=[('selected', '#d8e9ff')], foreground=[('selected', '#173253')])
        style.configure('TScrollbar', arrowsize=13)
        style.configure('TCheckbutton', background=COLORS['surface'], foreground=COLORS['ink'],
                        font=('Segoe UI', 9))

    def _img(self, key: str):
        if key not in self.photos:
            path = ASSETS / f'{key}.png'
            self.photos[key] = (tk.PhotoImage(file=str(path)) if path.exists()
                                else tk.PhotoImage(data=ICON_PNG_DATA[key]) if key in ICON_PNG_DATA else None)
        return self.photos[key]

    def _button(self, parent, text, icon, command, color, pad=10, small=False):
        # Flat modern button with subtle hover/press feedback.
        button = tk.Button(parent, text='  ' + text, image=self._img(icon), compound='left',
                           command=command, bg=color, activebackground=color, fg='white',
                           activeforeground='white', font=('Segoe UI', 9 if small else 10, 'bold'),
                           relief='flat', cursor='hand2', padx=pad, pady=7 if small else 9,
                           highlightthickness=0, borderwidth=0, takefocus=True)
        hover = {
            '#1877F2': '#1266D6', '#6558F5': '#5145D8', '#5145D8': '#4338CA',
            '#16A34A': '#15803D', '#D94645': '#BE3434', '#F59E0B': '#D97706',
            '#0F766E': '#115E59', '#E54868': '#BE3455', '#059F8E': '#0D887B',
        }
        darker = hover.get(str(color).upper(), color)
        button.bind('<Enter>', lambda _e, b=button, c=darker: b.configure(bg=c))
        button.bind('<Leave>', lambda _e, b=button, c=color: b.configure(bg=c))
        button.bind('<ButtonPress-1>', lambda _e, b=button, c=darker: b.configure(bg=c, relief='sunken'))
        button.bind('<ButtonRelease-1>', lambda _e, b=button, c=color: b.configure(bg=c, relief='flat'))
        return button

    def _card(self, parent):
        return tk.Frame(parent, bg=COLORS['surface'], highlightbackground=COLORS['line'],
                        highlightthickness=1, padx=14, pady=10)

    def _textlabel(self, parent, value, size=10, bold=False, color=None, **kw):
        return tk.Label(parent, text=value, bg=parent.cget('bg'),
                        fg=color or COLORS['ink'], font=('Segoe UI', size, 'bold' if bold else 'normal'),
                        anchor='w', **kw)

    def _build(self):
        # PostFlow shell: sidebar + top bar + content notebook. Existing posting,
        # scheduling, history and storage handlers are preserved below.
        self.root.configure(bg='#F4F7FC')
        shell = tk.Frame(self.root, bg='#F4F7FC')
        shell.pack(fill='both', expand=True)

        sidebar = tk.Frame(shell, bg='#101D3A', width=236)
        sidebar.pack(side='left', fill='y')
        sidebar.pack_propagate(False)
        main = tk.Frame(shell, bg='#F4F7FC')
        main.pack(side='left', fill='both', expand=True)
        header = tk.Frame(main, bg='white', height=58, highlightbackground='#E2E8F0', highlightthickness=1)
        header.pack(side='top', fill='x')
        header.pack_propagate(False)
        self.menu_toggle = tk.Button(header, text='☰', command=self._toggle_sidebar, bg='white', fg='#64748B',
                 activebackground='#EEF2FF', relief='flat', bd=0, font=('Segoe UI', 15, 'bold'), cursor='hand2', padx=8)
        self.menu_toggle.pack(side='left', padx=(14, 4))
        self.breadcrumb = tk.Label(header, text='PostFlow   ›   Tổng quan', bg='white', fg='#64748B',
                 font=('Segoe UI', 10))
        self.breadcrumb.pack(side='left', padx=8)
        search_box = tk.Frame(header, bg='white', highlightbackground='#CBD5E1', highlightthickness=1)
        search_box.pack(side='right', padx=(8, 18), pady=10)
        self.quick_search_var = tk.StringVar()
        search_entry = tk.Entry(search_box, textvariable=self.quick_search_var, width=24,
                                relief='flat', bd=0, font=('Segoe UI', 9),
                                fg='#334155', insertbackground='#334155')
        search_entry.pack(side='left', padx=(10, 4), ipady=6)
        search_entry.insert(0, 'Tìm bài / ngày / giờ / nhóm (Ctrl+F)...')
        search_entry.bind('<FocusIn>', lambda _e: search_entry.delete(0, 'end')
                          if search_entry.get() == 'Tìm bài / ngày / giờ / nhóm (Ctrl+F)...' else None)
        search_entry.bind('<Return>', lambda _e: self.open_global_search(self.quick_search_var.get()))
        tk.Button(search_box, text='⌕', command=lambda: self.open_global_search(self.quick_search_var.get()),
                  bg='white', fg='#475569', relief='flat', bd=0, cursor='hand2',
                  font=('Segoe UI', 12, 'bold'), padx=8).pack(side='right')
        tk.Label(header, text='●  Facebook: quản lý kết nối trong Cài đặt', bg='#F0FDFA', fg='#0F766E',
                 font=('Segoe UI', 9, 'bold'), padx=10, pady=7).pack(side='right', padx=6)

        statusline = tk.Frame(main, bg='#F4F7FC')
        statusline.pack(side='bottom', fill='x', padx=18, pady=(4, 8))
        tk.Label(statusline, text='●', font=('Segoe UI', 13), fg=COLORS['green'],
                 bg='#F4F7FC').pack(side='left')
        tk.Label(statusline, textvariable=self.status_text, font=('Segoe UI', 9, 'bold'),
                 fg=COLORS['ink'], bg='#F4F7FC', anchor='w').pack(side='left', padx=8)
        outer = tk.Frame(main, bg='#F4F7FC')
        outer.pack(fill='both', expand=True, padx=16, pady=12)
        # Hide the default notebook tabs: navigation is handled by the Canva-style sidebar.
        try:
            style = ttk.Style(self.root)
            style.layout('PostFlow.Hidden.TNotebook.Tab', [])
            style.configure('PostFlow.Hidden.TNotebook', background='#F5F8FD', borderwidth=0, tabmargins=0)
        except tk.TclError:
            pass
        self.tabs = ttk.Notebook(outer, style='PostFlow.Hidden.TNotebook')
        self.tabs.pack(fill='both', expand=True)

        self.dashboardtab = tk.Frame(self.tabs, bg='#F4F7FC')
        self.worktab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.histtab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.activitytab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.plantab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.settingtab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.grouptab = tk.Frame(self.tabs, bg=COLORS['bg'])
        self.tabs.add(self.dashboardtab, text='  Tổng quan  ')
        self.tabs.add(self.worktab, text='  Đăng bài  ')
        self.tabs.add(self.histtab, text='  Lịch sử đăng bài  ')
        self.tabs.add(self.activitytab, text='  Nhật ký hoạt động  ')
        self.tabs.add(self.plantab, text='  Lịch hẹn đăng  ')
        self.tabs.add(self.settingtab, text='  Cài đặt  ')
        self.tabs.add(self.grouptab, text='  Nhóm Facebook  ')

        nav_items = [
            ('▦   Tổng quan', self.dashboardtab),
            ('➤   Đăng bài', self.worktab),
            ('♧   Nhóm Facebook', self.grouptab),
            ('☷   Lịch sử đăng bài', self.histtab),
            ('∿   Nhật ký hoạt động', self.activitytab),
            ('◷   Lịch hẹn đăng', self.plantab),
        ]
        self.sidebar = sidebar
        self.sidebar_expanded = True
        self.nav_buttons = []
        for index, (label, target) in enumerate(nav_items):
            b = tk.Button(sidebar, text=label, anchor='w', padx=16, pady=11,
                          bg='#282B64' if index == 0 else '#101D3A', fg='white' if index == 0 else '#DCE5FA',
                          activebackground='#303B80', activeforeground='white', relief='flat', bd=0,
                          font=('Segoe UI', 10, 'bold' if index == 0 else 'normal'), cursor='hand2',
                          command=lambda page=target, text=label: self._navigate(page, text))
            b.pack(fill='x', padx=8, pady=2)
            self.nav_buttons.append((b, label))
        self.sidebar_account_heading = tk.Label(sidebar, text='TÀI KHOẢN', bg='#101D3A', fg='#91A0C5',
                 font=('Segoe UI', 8, 'bold'))
        self.sidebar_account_heading.pack(anchor='w', padx=18, pady=(20, 8))
        settings_label = '⚙   Cài đặt'
        settings_button = tk.Button(sidebar, text=settings_label, anchor='w', padx=16, pady=10,
                      bg='#101D3A', fg='#DCE5FA', activebackground='#303B80',
                      activeforeground='white', relief='flat', bd=0, font=('Segoe UI', 10), cursor='hand2',
                      command=lambda: self._navigate(self.settingtab, settings_label))
        settings_button.pack(fill='x', padx=8, pady=1)
        self.settings_nav_button = settings_button
        self.sidebar_settings_button = settings_button

        # Dashboard PostFlow: responsive, scrollable, with actions connected to real app pages.
        dash_wrap = tk.Frame(self.dashboardtab, bg='#F4F7FC')
        dash_wrap.pack(fill='both', expand=True)
        dash_canvas = tk.Canvas(dash_wrap, bg='#F4F7FC', highlightthickness=0, bd=0)
        dash_scroll = ttk.Scrollbar(dash_wrap, orient='vertical', command=dash_canvas.yview)
        dash_canvas.configure(yscrollcommand=dash_scroll.set)
        dash_scroll.pack(side='right', fill='y')
        dash_canvas.pack(side='left', fill='both', expand=True)
        dash = tk.Frame(dash_canvas, bg='#F4F7FC', padx=24, pady=22)
        dash_window = dash_canvas.create_window((0, 0), window=dash, anchor='nw')
        dash.bind('<Configure>', lambda _e: dash_canvas.configure(scrollregion=dash_canvas.bbox('all')))
        dash_canvas.bind('<Configure>', lambda e: dash_canvas.itemconfigure(dash_window, width=e.width))
        def _dashboard_wheel(event):
            if getattr(event, 'delta', 0):
                dash_canvas.yview_scroll(int(-event.delta / 120), 'units')
            elif getattr(event, 'num', None) == 4:
                dash_canvas.yview_scroll(-3, 'units')
            elif getattr(event, 'num', None) == 5:
                dash_canvas.yview_scroll(3, 'units')
        dash_canvas.bind('<MouseWheel>', _dashboard_wheel)
        dash_canvas.bind('<Button-4>', _dashboard_wheel)
        dash_canvas.bind('<Button-5>', _dashboard_wheel)

        title_row = tk.Frame(dash, bg='#F4F7FC')
        title_row.pack(fill='x', pady=(0, 18))
        title_copy = tk.Frame(title_row, bg='#F4F7FC')
        title_copy.pack(side='left', fill='x', expand=True)
        tk.Label(title_copy, text='TỔNG QUAN', bg='#F4F7FC', fg='#6558F5',
                 font=('Segoe UI', 8, 'bold')).pack(anchor='w', pady=(0, 5))
        tk.Label(title_copy, text='Chào mừng đến với PostFlow', bg='#F4F7FC', fg='#14213D',
                 font=('Segoe UI', 23, 'bold')).pack(anchor='w')
        tk.Label(title_copy, text='Theo dõi bài đăng, nhóm Facebook và các tác vụ cần xử lý tại một nơi.',
                 bg='#F4F7FC', fg='#64748B', font=('Segoe UI', 10)).pack(anchor='w', pady=(5, 0))
        action_row = tk.Frame(title_row, bg='#F4F7FC')
        action_row.pack(side='right', anchor='s', padx=(12, 0))
        self._button(action_row, 'Tạo bài đăng', 'send',
                     lambda: self._navigate(self.worktab, '➤   Đăng bài'), '#6558F5', small=True).pack(side='left', padx=(0, 8))
        tk.Button(action_row, text='↻  Làm mới', command=self.refresh_jobs,
                  bg='white', fg='#334155', activebackground='#EEF2FF', relief='flat',
                  highlightthickness=1, highlightbackground='#DCE4F0', cursor='hand2',
                  font=('Segoe UI', 9, 'bold'), padx=12, pady=9).pack(side='left')

        # Quick actions provide meaningful entry points to existing features.
        quick = tk.Frame(dash, bg='#F4F7FC')
        quick.pack(fill='x', pady=(0, 16))
        quick_actions = [
            ('✎', 'Soạn bài mới', 'Viết nội dung, thêm ảnh và chọn nhóm', lambda: self._navigate(self.worktab, '➤   Đăng bài'), '#EEF2FF', '#5548D9'),
            ('♧', 'Quản lý nhóm', 'Kiểm tra danh sách nhóm đã nhập', lambda: self._navigate(self.worktab, '♧   Nhóm Facebook'), '#E6FFFB', '#0F8B83'),
            ('☷', 'Lịch sử đăng', 'Xem trạng thái, ghi chú và bằng chứng', lambda: self._navigate(self.histtab, '☷   Lịch sử đăng bài'), '#FFF7E6', '#B7791F'),
            ('◷', 'Lịch hẹn đăng', 'Mở danh sách bản nháp và lịch đăng', lambda: self._navigate(self.plantab, '◷   Lịch hẹn đăng'), '#F3E8FF', '#7E22CE'),
        ]
        for icon, heading, sub, command, tint, accent in quick_actions:
            tile = tk.Frame(quick, bg='white', highlightbackground='#E2E8F0', highlightthickness=1, padx=13, pady=12, cursor='hand2')
            tile.pack(side='left', fill='both', expand=True, padx=(0, 9))
            tk.Label(tile, text=icon, bg=tint, fg=accent, font=('Segoe UI Symbol', 15, 'bold'), width=3, pady=5).pack(anchor='w')
            tk.Label(tile, text=heading, bg='white', fg='#172554', font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(9, 3))
            tk.Label(tile, text=sub, bg='white', fg='#64748B', font=('Segoe UI', 8), wraplength=190, justify='left').pack(anchor='w')
            for widget in tile.winfo_children():
                widget.bind('<Button-1>', lambda _e, cmd=command: cmd())
            tile.bind('<Button-1>', lambda _e, cmd=command: cmd())

        cards = tk.Frame(dash, bg='#F4F7FC')
        cards.pack(fill='x')
        self.dashboard_values = {}
        card_specs = [
            ('Tổng công việc', 'total', '#625BEE', 'Tất cả bài × nhóm đã ghi nhận', '▤', '#EEF2FF'),
            ('Thành công', 'success', '#059F8E', 'Facebook đã phản hồi hoặc tìm thấy bài đăng', '✓', '#E6FFFB'),
            ('Chờ / cần đối chiếu', 'review', '#D97706', 'Chờ duyệt hoặc chưa rõ kết quả', '◷', '#FFF7E6'),
            ('Lỗi / cần xử lý', 'failed', '#E54868', 'Công việc không gửi được', '!', '#FFF1F2'),
        ]
        for title, key, color, caption, glyph, tint in card_specs:
            card = tk.Frame(cards, bg='white', padx=16, pady=14,
                            highlightbackground='#E2E8F0', highlightthickness=1)
            card.pack(side='left', fill='both', expand=True, padx=(0, 10))
            top_line = tk.Frame(card, bg='white')
            top_line.pack(fill='x')
            tk.Label(top_line, text=glyph, bg=tint, fg=color, width=3, pady=5,
                     font=('Segoe UI', 11, 'bold')).pack(side='left')
            tk.Label(top_line, text=title, bg='white', fg='#64748B',
                     font=('Segoe UI', 9, 'bold')).pack(side='left', padx=9)
            value = tk.Label(card, text='—', bg='white', fg=color, font=('Segoe UI', 25, 'bold'))
            value.pack(anchor='w', pady=(10, 1))
            tk.Label(card, text=caption, bg='white', fg='#94A3B8',
                     font=('Segoe UI', 8), wraplength=210, justify='left').pack(anchor='w')
            self.dashboard_values[key] = value

        self.dashboard_period_summary = tk.Label(
            dash, text='Đang tổng hợp dữ liệu...', bg='#F4F7FC', fg='#475569',
            font=('Segoe UI', 9), anchor='w')
        self.dashboard_period_summary.pack(fill='x', pady=(8, 0))

        lower = tk.Frame(dash, bg='#F4F7FC')
        lower.pack(fill='x', pady=(16, 0))
        chart_card = tk.Frame(lower, bg='white', padx=17, pady=15,
                              highlightbackground='#E2E8F0', highlightthickness=1)
        chart_card.pack(side='left', fill='both', expand=True, padx=(0, 12))
        chart_head = tk.Frame(chart_card, bg='white')
        chart_head.pack(fill='x')
        tk.Label(chart_head, text='Tình hình xử lý bài đăng', bg='white', fg='#14213D',
                 font=('Segoe UI', 13, 'bold')).pack(side='left')
        tk.Label(chart_head, text='Dữ liệu thực tế lưu trên máy', bg='white', fg='#64748B',
                 font=('Segoe UI', 8)).pack(side='right')
        chart_legend = tk.Frame(chart_card, bg='white')
        chart_legend.pack(anchor='w', pady=(8, 0))
        for dot, text_value, color in [('●', 'Thành công', '#625BEE'), ('●', 'Đang xử lý', '#20B6D2'), ('●', 'Lỗi', '#F87171')]:
            tk.Label(chart_legend, text=dot, bg='white', fg=color, font=('Segoe UI', 9)).pack(side='left')
            tk.Label(chart_legend, text=text_value, bg='white', fg='#64748B', font=('Segoe UI', 8)).pack(side='left', padx=(2, 12))
        chart = tk.Canvas(chart_card, height=220, bg='white', highlightthickness=0)
        chart.pack(fill='both', expand=True, pady=(5, 0))
        def draw_dashboard_chart(_event=None):
            chart.delete('all')
            width = max(chart.winfo_width(), 340)
            height = max(chart.winfo_height(), 180)
            baseline = height - 34
            chart.create_line(12, baseline, width-12, baseline, fill='#E2E8F0')
            jobs_now = self.store.all(limit=1000000)
            values = [
                ('Thành công', sum(j.status in (CONFIRMED, OBSERVED, ACKNOWLEDGED) for j in jobs_now), '#625BEE'),
                ('Chờ / đối chiếu', sum(j.status in (PENDING, REVIEW, QUEUED, PREPARING, SUBMITTING) for j in jobs_now), '#20B6D2'),
                ('Lỗi', sum(j.status == FAILED for j in jobs_now), '#F87171'),
            ]
            max_value = max([v for _, v, _ in values] + [1])
            slot = (width - 36) / len(values)
            for i, (label, value, color) in enumerate(values):
                x = 18 + slot*i + slot/2
                bar_h = max(3, (height-78) * value / max_value) if value else 3
                chart.create_rectangle(x-22, baseline-bar_h, x+22, baseline, fill=color, outline='', width=0)
                chart.create_text(x, baseline-bar_h-12, text=str(value), fill='#334155', font=('Segoe UI', 9, 'bold'))
                chart.create_text(x, baseline+16, text=label, fill='#64748B', font=('Segoe UI', 8))
        chart.bind('<Configure>', draw_dashboard_chart)

        system = tk.Frame(lower, bg='white', padx=17, pady=15,
                          highlightbackground='#E2E8F0', highlightthickness=1, width=310)
        system.pack(side='left', fill='y')
        system.pack_propagate(False)
        system_head = tk.Frame(system, bg='white')
        system_head.pack(fill='x')
        tk.Label(system_head, text='Trạng thái hệ thống', bg='white', fg='#14213D',
                 font=('Segoe UI', 13, 'bold')).pack(side='left')
        tk.Label(system_head, text='● CỤC BỘ', bg='#E6FFFB', fg='#0F766E',
                 font=('Segoe UI', 8, 'bold'), padx=7, pady=4).pack(side='right')
        tk.Label(system, text='Thông tin từ ứng dụng hiện tại', bg='white', fg='#64748B',
                 font=('Segoe UI', 9)).pack(anchor='w', pady=(5, 10))
        self.dashboard_system_labels = {}
        for key, label, detail in [
            ('db', 'Cơ sở dữ liệu', 'SQLite · lưu trên máy'),
            ('scheduler', 'Bộ lập lịch', 'Tự kiểm tra bài đến hạn'),
            ('browser', 'Trình duyệt Facebook', 'Phiên đăng nhập cần được kiểm tra'),
        ]:
            line = tk.Frame(system, bg='white')
            line.pack(fill='x', pady=7)
            tk.Label(line, text=label, bg='white', fg='#334155', font=('Segoe UI', 9, 'bold')).pack(anchor='w')
            detail_label = tk.Label(line, text=detail, bg='white', fg='#0F766E', font=('Segoe UI', 8), anchor='w')
            detail_label.pack(anchor='w', pady=(3, 0))
            self.dashboard_system_labels[key] = detail_label
            tk.Frame(system, bg='#E2E8F0', height=1).pack(fill='x', pady=(4, 0))
        tk.Button(system, text='Xem lịch sử đăng bài  ›',
                  command=lambda: self._navigate(self.histtab, '☷   Lịch sử đăng bài'), bg='#F8FAFC', fg='#4338CA',
                  activebackground='#EEF2FF', relief='flat', bd=0, cursor='hand2',
                  font=('Segoe UI', 9, 'bold'), pady=9).pack(fill='x', pady=(7, 0))

        recent = tk.Frame(dash, bg='white', padx=17, pady=14,
                          highlightbackground='#E2E8F0', highlightthickness=1)
        recent.pack(fill='x', pady=(16, 0))
        recent_head = tk.Frame(recent, bg='white')
        recent_head.pack(fill='x', pady=(0, 9))
        tk.Label(recent_head, text='Hoạt động gần đây', bg='white', fg='#14213D',
                 font=('Segoe UI', 13, 'bold')).pack(side='left')
        tk.Label(recent_head, text='Các công việc mới nhất trong lịch sử', bg='white', fg='#64748B',
                 font=('Segoe UI', 8)).pack(side='left', padx=10)
        tk.Button(recent_head, text='Xem tất cả  ↗',
                  command=lambda: self._navigate(self.histtab, '☷   Lịch sử đăng bài'), bg='white', fg='#4338CA',
                  activebackground='#EEF2FF', relief='flat', bd=0, cursor='hand2',
                  font=('Segoe UI', 9, 'bold')).pack(side='right')
        self.dashboard_recent = tk.Frame(recent, bg='white')
        self.dashboard_recent.pack(fill='x')
        tk.Label(self.dashboard_recent, text='Chưa có công việc. Chọn “Soạn bài mới” để bắt đầu.',
                 bg='white', fg='#64748B', font=('Segoe UI', 9)).pack(anchor='w', pady=12)
        self.dashboard_refresh_chart = draw_dashboard_chart
        draw_dashboard_chart()

        # Khung cuon doc cho tab soan bai: tren man hinh thap van truy cap duoc
        # cac nut Thao tac/Ket qua bang cach cuon chuot hoac keo thanh cuon.
        work_canvas_wrap = tk.Frame(self.worktab, bg=COLORS['bg'])
        work_canvas_wrap.pack(fill='both', expand=True)
        work_canvas = tk.Canvas(work_canvas_wrap, bg=COLORS['bg'], highlightthickness=0)
        work_vscroll = ttk.Scrollbar(work_canvas_wrap, orient='vertical', command=work_canvas.yview)
        work_canvas.configure(yscrollcommand=work_vscroll.set)
        work_vscroll.pack(side='right', fill='y')
        work_canvas.pack(side='left', fill='both', expand=True)
        self.workcontent = tk.Frame(work_canvas, bg=COLORS['bg'])
        work_window = work_canvas.create_window((0, 0), window=self.workcontent, anchor='nw')
        def _sync_work_scrollregion(_event=None):
            work_canvas.configure(scrollregion=work_canvas.bbox('all'))
        def _sync_work_width(event):
            work_canvas.itemconfigure(work_window, width=event.width)
        self.workcontent.bind('<Configure>', _sync_work_scrollregion)
        work_canvas.bind('<Configure>', _sync_work_width)
        def _work_mousewheel(event):
            if getattr(event, 'delta', 0):
                work_canvas.yview_scroll(int(-event.delta / 120), 'units')
            elif getattr(event, 'num', None) == 4:
                work_canvas.yview_scroll(-3, 'units')
            elif getattr(event, 'num', None) == 5:
                work_canvas.yview_scroll(3, 'units')
        work_canvas.bind('<MouseWheel>', _work_mousewheel)
        work_canvas.bind('<Button-4>', _work_mousewheel)
        work_canvas.bind('<Button-5>', _work_mousewheel)
        top = tk.Frame(self.workcontent, bg=COLORS['bg'])
        top.pack(fill='x', expand=False)
        top.columnconfigure(0, weight=6)
        top.columnconfigure(1, weight=4)
        left = self._card(top)
        left.grid(row=0, column=0, sticky='nsew', padx=(0, 10))
        right = self._card(top)
        right.grid(row=0, column=1, sticky='nsew')
        self._textlabel(left, '① SOẠN BÀI VIẾT', 13, True).pack(anchor='w')
        self._textlabel(left, 'Nội dung sẽ giống nhau ở mọi nhóm. Giữ nguyên tiếng Việt và emoji.',
                        9, color=COLORS['muted']).pack(anchor='w', pady=(3, 9))
        self.content_input = ScrolledText(left, wrap='word', undo=True, height=7, font=('Segoe UI', 10),
                                          fg=COLORS['ink'], bg='#f9fbff', insertbackground=COLORS['ink'],
                                          relief='flat', highlightthickness=1,
                                          highlightbackground='#cbd7e7', padx=12, pady=10)
        self.content_input.pack(fill='x', expand=False)
        self.content_input.bind('<KeyRelease>', lambda _: self.queue_save())
        photo_line = tk.Frame(left, bg='white')
        photo_line.pack(fill='x', pady=(8, 4))
        self._button(photo_line, 'Thêm ảnh', 'photo', self.add_images, COLORS['purple'], small=True).pack(side='left')
        self._button(photo_line, 'Xóa ảnh', 'delete', self.remove_images, COLORS['red'], small=True).pack(side='left', padx=7)
        self.photo_label = self._textlabel(photo_line, 'Chưa có ảnh', 9, color=COLORS['muted'])
        self.photo_label.pack(side='left', padx=8)
        # Visual gallery: display the actual selected images, not only filenames/paths.
        preview_wrap = tk.Frame(left, bg='white', highlightthickness=1,
                                highlightbackground='#e1e8f2')
        preview_wrap.pack(fill='x', pady=(4, 0))
        self.photo_canvas = tk.Canvas(preview_wrap, height=172, bg='#f9fbff',
                                      highlightthickness=0, borderwidth=0)
        self.photo_scrollbar = ttk.Scrollbar(preview_wrap, orient='vertical',
                                             command=self.photo_canvas.yview)
        self.photo_canvas.configure(yscrollcommand=self.photo_scrollbar.set)
        self.photo_scrollbar.pack(side='right', fill='y')
        self.photo_canvas.pack(side='left', fill='both', expand=True)
        self.photo_preview = tk.Frame(self.photo_canvas, bg='#f9fbff')
        self._photo_window = self.photo_canvas.create_window(
            (0, 0), window=self.photo_preview, anchor='nw')
        self.photo_preview.bind('<Configure>', lambda _e: self.photo_canvas.configure(
            scrollregion=self.photo_canvas.bbox('all')))
        self.photo_canvas.bind('<Configure>', lambda e: self.photo_canvas.itemconfigure(
            self._photo_window, width=e.width))
        self._textlabel(right, '② DANH SÁCH NHÓM FACEBOOK', 13, True).pack(anchor='w')
        self._textlabel(right, 'Mỗi đường dẫn một dòng. Có thể thêm nhiều nhóm; ứng dụng sẽ hỏi trước khi xử lý link trùng.',
                        9, color=COLORS['muted']).pack(anchor='w', pady=(3, 9))
        self.groups_input = ScrolledText(right, wrap='none', height=10, font=('Consolas', 10),
                                         fg=COLORS['ink'], bg='#f9fbff', relief='flat',
                                         highlightthickness=1, highlightbackground='#cbd7e7',
                                         padx=10, pady=9)
        self.groups_input.pack(fill='x')
        self.groups_input.bind('<KeyRelease>', lambda _: (self.queue_save(), self.root.after(250, self._refresh_group_list())))
        group_actions = tk.Frame(right, bg='white')
        group_actions.pack(fill='x', pady=(7, 0))
        tk.Button(group_actions, text='+ Thêm nhiều nhóm', command=self.add_group_links,
                  bg='#6558F5', fg='white', relief='flat', cursor='hand2',
                  font=('Segoe UI', 9, 'bold'), padx=12, pady=7).pack(side='left')
        tk.Button(group_actions, text='Kiểm tra link trùng', command=self.check_duplicate_groups,
                  bg='#EEF2FF', fg='#4338CA', relief='flat', cursor='hand2',
                  font=('Segoe UI', 9, 'bold'), padx=12, pady=7).pack(side='left', padx=7)
        setting_line = tk.Frame(right, bg='white')
        setting_line.pack(fill='x', pady=(9, 4))
        self._textlabel(setting_line, 'Nghỉ giữa các bài đã gửi (giây, tối thiểu 1800):', 9, True).pack(side='left')
        ttk.Combobox(setting_line, textvariable=self.delay_seconds, values=['1800', '1860', '2700', '3600'],
                     state='readonly', width=6).pack(side='left', padx=8)
        self.delay_seconds.trace_add('write', lambda *_: self.queue_save())
        ttk.Checkbutton(right,
                        text='TỰ ĐỘNG chuyển nhóm tiếp theo (kể cả khi nhóm hiện tại lỗi)',
                        variable=self.autocontinue).pack(anchor='w', pady=(9, 3))
        tk.Label(right, text='Nhóm lỗi / chưa rõ: chuyển NGAY, không nghỉ. Chỉ nghỉ sau bài có tín hiệu gửi. '
                 'Đã nhấn Đăng không có nghĩa đã được xuất bản.',
                 bg='white', fg=COLORS['green'], font=('Segoe UI', 9),
                 justify='left', wraplength=395).pack(anchor='w')
        tk.Label(right, text=f'Không giới hạn số nhóm/lượt · tối đa {MAX_IMAGES} ảnh/bài. Chỉ đăng nhóm bạn được phép.',
                 bg='white', fg=COLORS['muted'], font=('Segoe UI', 9),
                 justify='left', wraplength=395).pack(anchor='w', pady=(11, 0))

        commands = self._card(self.workcontent)
        commands.pack(fill='x', pady=(9, 0))
        self._textlabel(commands, '③ THAO TÁC', 12, True).pack(anchor='w', pady=(0, 8))
        # Split controls across two rows so buttons never run off the right edge
        # on smaller Windows displays or when display scaling is above 100%.
        line1 = tk.Frame(commands, bg='white')
        line1.pack(fill='x', pady=(0, 5))
        self._button(line1, '1. Đăng nhập Facebook', 'browser', self.login, COLORS['primary']).pack(side='left', padx=(0, 8))
        self._button(line1, '2. ĐĂNG & TỰ CHUYỂN NHÓM', 'send', self.start_new, COLORS['green']).pack(side='left', padx=(0, 8))
        line2 = tk.Frame(commands, bg='white')
        line2.pack(fill='x')
        self._button(line2, 'Dừng sau nhóm này', 'stop', self.stop_after_current, COLORS['red'], small=True).pack(side='left', padx=(0, 8))
        self._button(line2, 'Tiếp tục nhóm còn lại', 'resume', self.resume_queue, COLORS['purple'], small=True).pack(side='left', padx=(0, 8))
        self._button(line2, 'Cài/Sửa Playwright', 'refresh', self.install_dependencies, COLORS['orange'], small=True).pack(side='left')
        self._textlabel(commands, 'Lần đầu hãy mở Chrome đăng nhập tài khoản. Ứng dụng không yêu cầu mật khẩu.',
                        9, color=COLORS['muted']).pack(anchor='w', pady=(10, 0))
        history = self._card(self.histtab)
        history.pack(fill='both', expand=True, padx=0, pady=0)
        hhead = tk.Frame(history, bg='white')
        hhead.pack(fill='x', pady=(0, 8))
        self._textlabel(hhead, '④ KẾT QUẢ THEO TỪNG NHÓM', 12, True).pack(side='left')
        self._button(hhead, 'Làm mới', 'refresh', self.refresh_jobs, COLORS['primary'], small=True).pack(side='right')
        frame = tk.Frame(history, bg='white')
        frame.pack(fill='both', expand=True)
        self.tree = ttk.Treeview(frame, columns=('id', 'group', 'datetime', 'state', 'reason'),
                                 show='headings', style='Jobs.Treeview', height=5)
        cols = {'id': ('ID', 50), 'group': ('Nhóm Facebook', 290),
                'datetime': ('Ngày/giờ đăng', 145),
                'state': ('Trạng thái', 185), 'reason': ('Chi tiết', 520)}
        for name, (heading, width) in cols.items():
            self.tree.heading(name, text=heading)
            self.tree.column(name, width=width, minwidth=55, stretch=(name in {'group', 'reason'}))
        self.tree.tag_configure('done', background='#e9f8ee')
        self.tree.tag_configure('issue', background='#fff0ee')
        self.tree.tag_configure('review', background='#fff8e5')
        self.tree.tag_configure('normal', background='#ffffff')
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.tree.yview)
        scrollbar.pack(side='right', fill='y')
        # Thanh cuộn ngang cho phép kéo sang phải để đọc phần Chi tiết dài.
        hscrollbar = ttk.Scrollbar(history, orient='horizontal', command=self.tree.xview)
        hscrollbar.pack(side='bottom', fill='x')
        self.tree.configure(yscrollcommand=scrollbar.set, xscrollcommand=hscrollbar.set)
        self.tree.bind('<Double-Button-1>', lambda _: self.view_history_details())
        tools = tk.Frame(history, bg='white')
        tools.pack(fill='x', pady=(10, 6))
        self._button(tools, 'Xem chi tiết đầy đủ', 'check', self.view_history_details,
                     COLORS['primary'], small=True).pack(side='left', padx=(0, 6))
        self._button(tools, 'Mở nhóm đã chọn', 'browser', self.open_selected_group,
                     COLORS['primary'], small=True).pack(side='left', padx=(0, 6))
        self._button(tools, 'Ảnh chụp', 'photo', self.open_evidence,
                     COLORS['orange'], small=True).pack(side='left', padx=(0, 6))
        self._button(tools, 'ĐÃ THẤY BÀI (1 chạm)', 'check', self.confirm_job,
                     COLORS['green'], small=True).pack(side='left', padx=(0, 6))
        self._button(tools, 'Thử lại lỗi chưa gửi', 'refresh', self.retry_failed,
                     COLORS['purple'], small=True).pack(side='left', padx=(0, 6))
        self._button(tools, 'Dùng lại nội dung', 'resume', self.reuse_history_post,
                     COLORS['orange'], small=True).pack(side='left')
        tk.Label(history, textvariable=self.summary_text,
                 bg='white', fg=COLORS['primary'], font=('Segoe UI', 10, 'bold'), anchor='w').pack(fill='x', pady=(5, 2))
        tk.Label(history, text='Xanh: thấy bài/đã nhận · Vàng: chờ duyệt/chưa rõ · Đỏ: bỏ qua vì lỗi. '
                 'Nếu đã thấy bài đủ chữ và ảnh trên Facebook, chọn dòng và bấm ĐÃ THẤY BÀI.',
                 bg='white', fg=COLORS['muted'], font=('Segoe UI', 9), anchor='w').pack(fill='x', pady=(1, 4))
        self.console = ScrolledText(history, height=3, state='disabled', wrap='word',
                                    font=('Consolas', 9), fg='#24415c', bg='#f5f8fd',
                                    highlightthickness=0, borderwidth=0)
        self.console.pack(fill='x')

        # Nhật ký hoạt động: ghi lại thao tác và sự kiện của ứng dụng, tách khỏi lịch sử từng bài đăng.
        activity = self._card(self.activitytab)
        activity.pack(fill='both', expand=True, padx=0, pady=0)
        activity_head = tk.Frame(activity, bg='white')
        activity_head.pack(fill='x', pady=(0, 8))
        self._textlabel(activity_head, 'NHẬT KÝ HOẠT ĐỘNG ỨNG DỤNG', 12, True).pack(side='left')
        self._button(activity_head, 'Làm mới', 'refresh', self.refresh_activity,
                     COLORS['primary'], small=True).pack(side='right', padx=(6, 0))
        self._button(activity_head, 'Xóa nhật ký', 'stop', self.clear_activity_log,
                     COLORS['red'], small=True).pack(side='right')
        tk.Label(activity, text='Ghi lại thao tác trong ứng dụng như mở phiên, thay đổi cài đặt, làm mới dữ liệu và các thông báo xử lý. '
                 'Không dùng trang này để xem kết quả từng bài đăng.',
                 bg='white', fg=COLORS['muted'], font=('Segoe UI', 9), anchor='w',
                 wraplength=1100, justify='left').pack(fill='x', pady=(0, 8))
        activity_frame = tk.Frame(activity, bg='white')
        activity_frame.pack(fill='both', expand=True)
        self.activity_tree = ttk.Treeview(activity_frame,
            columns=('time', 'category', 'message'), show='headings',
            style='Jobs.Treeview', height=18)
        self.activity_tree.heading('time', text='Thời gian')
        self.activity_tree.heading('category', text='Loại hoạt động')
        self.activity_tree.heading('message', text='Nội dung sự kiện')
        self.activity_tree.column('time', width=175, minwidth=150, stretch=False)
        self.activity_tree.column('category', width=165, minwidth=130, stretch=False)
        self.activity_tree.column('message', width=700, minwidth=350, stretch=True)
        self.activity_tree.pack(side='left', fill='both', expand=True)
        activity_scroll = ttk.Scrollbar(activity_frame, orient='vertical',
                                        command=self.activity_tree.yview)
        activity_scroll.pack(side='right', fill='y')
        self.activity_tree.configure(yscrollcommand=activity_scroll.set)
        self.activity_tree.tag_configure('error', background='#fff0ee')
        self.activity_tree.tag_configure('normal', background='#ffffff')
        self._build_library_schedule_tab()
        self._build_settings_tab()
        self._build_groups_tab()

    def _build_settings_tab(self):
        page = tk.Frame(self.settingtab, bg='#F4F7FC', padx=24, pady=22)
        page.pack(fill='both', expand=True)
        tk.Label(page, text='CÀI ĐẶT', bg='#F4F7FC', fg='#6558F5',
                 font=('Segoe UI', 9, 'bold')).pack(anchor='w')
        tk.Label(page, text='Thiết lập PostFlow', bg='#F4F7FC', fg='#14213D',
                 font=('Segoe UI', 23, 'bold')).pack(anchor='w', pady=(3, 5))
        tk.Label(page, text='Quản lý kết nối Facebook và giao diện ứng dụng.',
                 bg='#F4F7FC', fg='#64748B', font=('Segoe UI', 10)).pack(anchor='w', pady=(0, 18))

        fb = tk.Frame(page, bg='white', highlightbackground='#E2E8F0', highlightthickness=1, padx=18, pady=17)
        fb.pack(fill='x', pady=(0, 14))
        tk.Label(fb, text='Kết nối tài khoản Facebook', bg='white', fg='#172554',
                 font=('Segoe UI', 13, 'bold')).pack(anchor='w')
        tk.Label(fb, text='Mở cửa sổ trình duyệt để bạn tự đăng nhập. PostFlow không yêu cầu nhập mật khẩu Facebook trong ứng dụng.',
                 bg='white', fg='#64748B', font=('Segoe UI', 9), wraplength=900,
                 justify='left').pack(anchor='w', pady=(6, 12))
        fb_actions = tk.Frame(fb, bg='white')
        fb_actions.pack(anchor='w')
        self._button(fb_actions, 'Đăng nhập Facebook', 'browser', self.login,
                     '#1877F2', small=True).pack(side='left', padx=(0, 8))
        self._button(fb_actions, 'Làm mới trạng thái', 'refresh', self.refresh_jobs,
                     '#64748B', small=True).pack(side='left')
        tk.Label(fb, text='Lưu ý: trạng thái đăng nhập được duy trì trong hồ sơ trình duyệt của ứng dụng.',
                 bg='white', fg='#0F766E', font=('Segoe UI', 9)).pack(anchor='w', pady=(12, 0))

        appearance = tk.Frame(page, bg='white', highlightbackground='#E2E8F0', highlightthickness=1, padx=18, pady=17)
        appearance.pack(fill='x', pady=(0, 14))
        tk.Label(appearance, text='Giao diện ứng dụng', bg='white', fg='#172554',
                 font=('Segoe UI', 13, 'bold')).pack(anchor='w')
        tk.Label(appearance, text='Chọn chế độ hiển thị phù hợp với môi trường làm việc.',
                 bg='white', fg='#64748B', font=('Segoe UI', 9)).pack(anchor='w', pady=(5, 12))
        self.theme_choice = tk.StringVar(value=getattr(self, '_theme_mode', 'Sáng'))
        theme_row = tk.Frame(appearance, bg='white')
        theme_row.pack(anchor='w')
        self.theme_light_button = tk.Button(theme_row, text='☀  Nền sáng', command=lambda: self._apply_theme('Sáng'),
                       bg='#6558F5', fg='white', activebackground='#5145D8', activeforeground='white',
                       relief='flat', bd=0, padx=18, pady=9, cursor='hand2', font=('Segoe UI', 10, 'bold'))
        self.theme_light_button.pack(side='left', padx=(0, 8))
        self.theme_dark_button = tk.Button(theme_row, text='☾  Nền tối', command=lambda: self._apply_theme('Tối'),
                       bg='#E8ECF5', fg='#172554', activebackground='#D8DEEA', activeforeground='#172554',
                       relief='flat', bd=0, padx=18, pady=9, cursor='hand2', font=('Segoe UI', 10, 'bold'))
        self.theme_dark_button.pack(side='left')
        tk.Label(appearance, text='Chế độ tối/sáng áp dụng cho khung chính và các vùng giao diện hỗ trợ.',
                 bg='white', fg='#64748B', font=('Segoe UI', 9)).pack(anchor='w', pady=(10, 0))

        data_panel = tk.Frame(page, bg='white', highlightbackground='#E2E8F0', highlightthickness=1, padx=18, pady=17)
        data_panel.pack(fill='x', pady=(0, 14))
        tk.Label(data_panel, text='Dữ liệu và sao lưu', bg='white', fg='#172554',
                 font=('Segoe UI', 13, 'bold')).pack(anchor='w')
        tk.Label(data_panel, text='Xuất lịch sử ra CSV để mở bằng Excel hoặc tạo bản sao lưu cơ sở dữ liệu SQLite và nhật ký.',
                 bg='white', fg='#64748B', font=('Segoe UI', 9), wraplength=900,
                 justify='left').pack(anchor='w', pady=(5, 12))
        data_actions = tk.Frame(data_panel, bg='white')
        data_actions.pack(anchor='w')
        self._button(data_actions, 'Xuất lịch sử CSV', 'download', self.export_jobs_csv,
                     '#0F766E', small=True).pack(side='left', padx=(0, 8))
        self._button(data_actions, 'Sao lưu dữ liệu', 'save', self.backup_data,
                     '#6558F5', small=True).pack(side='left', padx=(0, 8))
        self._button(data_actions, 'Mở thư mục dữ liệu', 'folder', self.open_data_folder,
                     '#64748B', small=True).pack(side='left')

        info = tk.Frame(page, bg='#EEF2FF', padx=16, pady=14)
        info.pack(fill='x')
        tk.Label(info, text='Bảo mật tài khoản', bg='#EEF2FF', fg='#3730A3',
                 font=('Segoe UI', 11, 'bold')).pack(anchor='w')
        tk.Label(info, text='Không lưu mật khẩu Facebook dưới dạng văn bản. Hãy đăng nhập trực tiếp trong cửa sổ trình duyệt.',
                 bg='#EEF2FF', fg='#4338CA', font=('Segoe UI', 9), wraplength=900,
                 justify='left').pack(anchor='w', pady=(5, 0))

    def _apply_theme(self, mode):
        """Switch between light and dark palettes and refresh the selection buttons."""
        dark = mode == 'Tối'
        self.theme_choice.set(mode)
        bg = '#111827' if dark else '#F4F7FC'
        surface = '#1F2937' if dark else '#FFFFFF'
        fg = '#F3F4F6' if dark else '#172554'
        muted = '#CBD5E1' if dark else '#64748B'
        self._theme_mode = mode
        try:
            self.store.set_app_state('theme', mode)
        except Exception:
            pass
        try:
            self.root.configure(bg=bg)
            for widget in self.root.winfo_children():
                self._recolor_theme(widget, dark, bg, surface, fg, muted)
            if hasattr(self, 'theme_light_button'):
                self.theme_light_button.configure(bg=('#6558F5' if not dark else '#374151'), fg='white' if not dark else '#E5E7EB')
                self.theme_dark_button.configure(bg=('#6558F5' if dark else '#E8ECF5'), fg='white' if dark else '#172554')
            self.status_text.set(f'Đã chuyển giao diện {mode.lower()}.')
        except tk.TclError:
            pass

    def _recolor_theme(self, widget, dark, bg, surface, fg, muted):
        """Apply readable text contrast in both themes without recoloring sidebar/action buttons."""
        try:
            cls = widget.winfo_class()
            current_bg = str(widget.cget('bg')) if 'bg' in widget.keys() else ''
            sidebar_colors = {'#101D3A', '#1A2A4D', '#282B64', '#303B80'}
            fixed_colors = {'#1877F2', '#6558F5', '#5145D8', '#16A34A', '#D94645',
                            '#F59E0B', '#0F766E', '#E54868', '#059F8E'}
            preserve = current_bg in sidebar_colors or current_bg in fixed_colors
            if not preserve and 'bg' in widget.keys():
                if current_bg.lower() in ('white', '#ffffff', '#fff', '#eef2ff', '#f0fdfa', '#e8eef7',
                                           '#e6fffb', '#fff7e6', '#fff1f2', '#f3e8ff', '#f9fbff', '#f5f8fd',
                                           '#f4f7fc'):
                    widget.configure(bg=surface if current_bg.lower() not in ('#f4f7fc', '#f9fbff', '#f5f8fd') else bg)
                elif current_bg in ('#111827', '#1F2937'):
                    widget.configure(bg=bg if current_bg == '#111827' else surface)
            if 'fg' in widget.keys():
                old_fg = str(widget.cget('fg')).lower()
                if preserve:
                    pass
                elif old_fg not in ('white', '#ffffff', '#fff', '#1877f2', '#6558f5', '#5145d8',
                                    '#16a34a', '#d94645', '#f59e0b', '#0f766e', '#e54868', '#059f8e'):
                    widget.configure(fg=( '#f9fafb' if dark else '#111827'))
                elif old_fg in ('white', '#ffffff', '#fff') and not dark:
                    widget.configure(fg='#111827')
            if cls in ('Text', 'Entry', 'Listbox'):
                widget.configure(bg=surface, fg=('#F9FAFB' if dark else '#111827'),
                                 insertbackground=('#F9FAFB' if dark else '#111827'))
                if cls == 'Listbox':
                    widget.configure(selectbackground='#374151' if dark else '#E8E7FF',
                                     selectforeground='#FFFFFF' if dark else '#111827')
            if cls == 'Canvas':
                widget.configure(bg=bg)
            for child in widget.winfo_children():
                self._recolor_theme(child, dark, bg, surface, fg, muted)
        except (tk.TclError, KeyError):
            pass

    def _toggle_sidebar(self):
        """Collapse the left navigation to icons and restore it on the next click."""
        self.sidebar_expanded = not self.sidebar_expanded
        expanded = self.sidebar_expanded
        self.sidebar.configure(width=236 if expanded else 76)
        # Brand labels and section headers are hidden in compact mode.
        for widget in getattr(self, 'sidebar_brand_widgets', []):
            try:
                if expanded:
                    widget.pack(anchor='w', padx=20 if 'PostFlow' in str(widget.cget('text')) else 22,
                                pady=(24, 0) if 'PostFlow' in str(widget.cget('text')) else (2, 24) if 'FACEBOOK' in str(widget.cget('text')) else (0, 8))
                else:
                    widget.pack_forget()
            except tk.TclError:
                pass
        for button, label in getattr(self, 'nav_buttons', []):
            icon = label.strip().split()[0]
            button.configure(text=icon if not expanded else label, anchor='center' if not expanded else 'w',
                             padx=4 if not expanded else 16)
            button.pack_configure(padx=5 if not expanded else 8)
        if hasattr(self, 'sidebar_account_heading'):
            if expanded:
                self.sidebar_account_heading.configure(text='TÀI KHOẢN', anchor='w')
                self.sidebar_account_heading.pack(anchor='w', padx=18, pady=(20, 8))
            else:
                self.sidebar_account_heading.configure(text='♙', anchor='center')
                self.sidebar_account_heading.pack(anchor='center', padx=0, pady=(18, 8))
        if hasattr(self, 'sidebar_settings_button'):
            self.sidebar_settings_button.configure(text='⚙' if not expanded else '⚙   Cài đặt',
                                                  anchor='center' if not expanded else 'w',
                                                  padx=4 if not expanded else 16)
            self.sidebar_settings_button.pack_configure(padx=5 if not expanded else 8)
        # Hide the bottom promo when compact; restore it when expanded.
        if hasattr(self, 'sidebar_promo'):
            if expanded:
                self.sidebar_promo.pack(side='bottom', fill='x', padx=10, pady=(10, 12))
            else:
                self.sidebar_promo.pack_forget()

    def _build_groups_tab(self):
        """Show saved/input group URLs and a visual mock preview when a row is selected."""
        page = tk.Frame(self.grouptab, bg='#F4F7FC', padx=22, pady=20)
        page.pack(fill='both', expand=True)
        tk.Label(page, text='NHÓM FACEBOOK', bg='#F4F7FC', fg='#6558F5',
                 font=('Segoe UI', 9, 'bold')).pack(anchor='w')
        tk.Label(page, text='Danh sách nhóm và xem trước mô phỏng', bg='#F4F7FC', fg='#14213D',
                 font=('Segoe UI', 21, 'bold')).pack(anchor='w', pady=(3, 5))
        tk.Label(page, text='Chọn một đường dẫn để xem thẻ nhóm minh họa. Đây là bản xem trước giao diện, không phải dữ liệu trực tiếp từ Facebook.',
                 bg='#F4F7FC', fg='#64748B', font=('Segoe UI', 9), wraplength=1000,
                 justify='left').pack(anchor='w', pady=(0, 14))
        body = tk.Frame(page, bg='#F4F7FC')
        body.pack(fill='both', expand=True)
        left = tk.Frame(body, bg='white', highlightbackground='#DCE4F0', highlightthickness=1, padx=12, pady=12)
        left.pack(side='left', fill='both', expand=True, padx=(0, 10))
        right = tk.Frame(body, bg='white', highlightbackground='#DCE4F0', highlightthickness=1, padx=16, pady=16, width=390)
        right.pack(side='right', fill='both')
        right.pack_propagate(False)
        top = tk.Frame(left, bg='white')
        top.pack(fill='x', pady=(0, 8))
        tk.Label(top, text='Đường dẫn nhóm', bg='white', fg='#172554', font=('Segoe UI', 12, 'bold')).pack(side='left')
        self.group_search_var = tk.StringVar()
        group_search = tk.Entry(top, textvariable=self.group_search_var, width=27,
                                font=('Segoe UI', 9), relief='solid', bd=1)
        group_search.pack(side='left', padx=(12, 5), ipady=5)
        group_search.insert(0, 'Tìm link nhóm...')
        group_search.bind('<FocusIn>', lambda _e: group_search.delete(0, 'end')
                          if group_search.get() == 'Tìm link nhóm...' else None)
        group_search.bind('<KeyRelease>', lambda _e: self._refresh_group_list())
        self.group_count_label = tk.Label(top, text='0 nhóm', bg='#EEF2FF', fg='#5145D8',
                                          font=('Segoe UI', 8, 'bold'), padx=8, pady=5)
        self.group_count_label.pack(side='left', padx=5)
        tk.Button(top, text='↻ Đồng bộ danh sách', command=self._refresh_group_list,
                  bg='#6558F5', fg='white', relief='flat', cursor='hand2', padx=10, pady=6).pack(side='right')
        tk.Button(top, text='+ Thêm nhóm', command=self.add_group_links,
                  bg='#0F9F8F', fg='white', relief='flat', cursor='hand2',
                  padx=10, pady=6).pack(side='right', padx=7)
        tk.Button(top, text='Xóa nhóm đã chọn', command=self.delete_selected_group,
                  bg='#FFF1F2', fg='#BE3455', relief='flat', cursor='hand2',
                  padx=10, pady=6).pack(side='right', padx=7)
        tk.Button(top, text='Xuất danh sách', command=self.export_groups_txt,
                  bg='#E6FFFB', fg='#0F766E', relief='flat', cursor='hand2',
                  padx=10, pady=6).pack(side='right', padx=7)
        list_wrap = tk.Frame(left, bg='white')
        list_wrap.pack(fill='both', expand=True)
        self.group_listbox = tk.Listbox(list_wrap, font=('Segoe UI', 10), activestyle='none',
                                        selectbackground='#E8E7FF', selectforeground='#312E81',
                                        bg='#FFFFFF', fg='#334155', relief='flat', highlightthickness=1,
                                        highlightbackground='#E2E8F0')
        self.group_listbox.pack(side='left', fill='both', expand=True)
        sc = ttk.Scrollbar(list_wrap, orient='vertical', command=self.group_listbox.yview)
        sc.pack(side='right', fill='y')
        self.group_listbox.configure(yscrollcommand=sc.set)
        self.group_listbox.bind('<<ListboxSelect>>', self._show_group_preview)
        tk.Label(right, text='XEM TRƯỚC NHÓM', bg='white', fg='#6558F5', font=('Segoe UI', 9, 'bold')).pack(anchor='w')
        self.group_preview_icon = tk.Label(right, text='f', bg='#1877F2', fg='white',
                                           font=('Segoe UI', 24, 'bold'), width=3, height=1)
        self.group_preview_icon.pack(anchor='w', pady=(16, 10))
        self.group_preview_name = tk.Label(right, text='Chưa chọn nhóm', bg='white', fg='#172554',
                                           font=('Segoe UI', 15, 'bold'), wraplength=330, justify='left')
        self.group_preview_name.pack(anchor='w')
        self.group_preview_url = tk.Label(right, text='Chọn một đường dẫn ở danh sách bên trái.', bg='white',
                                          fg='#64748B', font=('Segoe UI', 9), wraplength=330, justify='left')
        self.group_preview_url.pack(anchor='w', pady=(8, 14))
        self.group_preview_status = tk.Label(right, text='● Bản xem trước mô phỏng', bg='#E6FFFB', fg='#0F766E',
                                             font=('Segoe UI', 9, 'bold'), padx=9, pady=6)
        self.group_preview_status.pack(anchor='w')
        tk.Button(right, text='Mở đường dẫn trong trình duyệt', command=self._open_preview_group,
                  bg='#1877F2', fg='white', relief='flat', cursor='hand2', padx=12, pady=9).pack(anchor='w', pady=(18, 0))
        self._refresh_group_list()

    def open_global_search(self, initial_query=''):
        """Search real posting records by content, group URL, date and time."""
        win = tk.Toplevel(self.root)
        win.title('Tìm kiếm dữ liệu PostFlow')
        win.geometry('1080x620')
        win.minsize(850, 480)
        win.configure(bg='#F4F7FC')
        win.transient(self.root)

        header = tk.Frame(win, bg='white', padx=16, pady=12)
        header.pack(fill='x')
        tk.Label(header, text='TÌM KIẾM BÀI ĐĂNG VÀ NHÓM FACEBOOK',
                 bg='white', fg='#172554', font=('Segoe UI', 13, 'bold')).pack(anchor='w')
        filters = tk.Frame(win, bg='#F4F7FC', padx=14, pady=12)
        filters.pack(fill='x')
        fields = {}
        specs = [
            ('keyword', 'Nội dung bài viết', 30),
            ('date', 'Ngày/tháng/năm (dd/mm/yyyy hoặc yyyy-mm-dd)', 22),
            ('time', 'Giờ đăng (vd. 14:30 hoặc 14)', 14),
            ('group', 'Link nhóm Facebook', 30),
        ]
        for idx, (key, label, width) in enumerate(specs):
            cell = tk.Frame(filters, bg='#F4F7FC')
            cell.grid(row=idx // 2, column=idx % 2, sticky='ew', padx=6, pady=5)
            filters.columnconfigure(idx % 2, weight=1)
            tk.Label(cell, text=label, bg='#F4F7FC', fg='#475569',
                     font=('Segoe UI', 8, 'bold')).pack(anchor='w')
            var = tk.StringVar()
            ent = tk.Entry(cell, textvariable=var, font=('Segoe UI', 9), relief='solid', bd=1)
            ent.pack(fill='x', ipady=5)
            fields[key] = var
        fields['keyword'].set(initial_query if initial_query != 'Tìm bài / ngày / giờ / nhóm (Ctrl+F)...' else '')
        tk.Label(filters, text='Trạng thái', bg='#F4F7FC', fg='#475569',
                 font=('Segoe UI', 8, 'bold')).grid(row=2, column=0, sticky='w', padx=6, pady=(5, 0))
        status_var = tk.StringVar(value='Tất cả')
        status_box = ttk.Combobox(filters, textvariable=status_var, state='readonly', width=30,
                                  values=['Tất cả'] + [f'{k} · {v}' for k, v in LABELS.items()])
        status_box.grid(row=3, column=0, sticky='ew', padx=6, pady=(2, 0))

        result_frame = tk.Frame(win, bg='white', padx=10, pady=10)
        result_frame.pack(fill='both', expand=True, padx=14, pady=(0, 14))
        cols = ('id', 'datetime', 'content', 'group', 'status', 'permalink')
        tree = ttk.Treeview(result_frame, columns=cols, show='headings', style='Jobs.Treeview')
        headings = {
            'id': ('ID', 55), 'datetime': ('Ngày và giờ', 145),
            'content': ('Nội dung bài', 330), 'group': ('Link nhóm', 260),
            'status': ('Trạng thái', 145), 'permalink': ('Link bài viết', 220)
        }
        for key, (label, width) in headings.items():
            tree.heading(key, text=label)
            tree.column(key, width=width, minwidth=60, stretch=(key in {'content', 'group', 'permalink'}))
        tree.pack(side='left', fill='both', expand=True)
        scroll = ttk.Scrollbar(result_frame, orient='vertical', command=tree.yview)
        scroll.pack(side='right', fill='y')
        tree.configure(yscrollcommand=scroll.set)
        footer = tk.Label(win, text='', bg='#F4F7FC', fg='#64748B',
                          font=('Segoe UI', 9), anchor='w')
        footer.pack(fill='x', padx=20, pady=(0, 8))
        tk.Button(win, text='Xuất kết quả tìm kiếm CSV', command=lambda: self.export_jobs_csv(
            job_ids=getattr(self, '_last_search_job_ids', None)), bg='#0F766E', fg='white',
            relief='flat', cursor='hand2', padx=13, pady=7,
            font=('Segoe UI', 9, 'bold')).pack(anchor='e', padx=18, pady=(0, 12))

        def run_search():
            tree.delete(*tree.get_children())
            jobs = self.store.all(limit=1000000)
            keyword = fields['keyword'].get().strip().casefold()
            date_q = fields['date'].get().strip()
            time_q = fields['time'].get().strip()
            group_q = fields['group'].get().strip().casefold()
            matched = []
            status_filter = status_var.get().split(' · ', 1)[0] if status_var.get() != 'Tất cả' else ''
            for job in jobs:
                if status_filter and job.status != status_filter:
                    continue
                stamp = job.sent_intent_at or job.created_at
                try:
                    dt = datetime.fromisoformat(stamp).astimezone()
                    date_display = dt.strftime('%d/%m/%Y')
                    time_display = dt.strftime('%H:%M:%S')
                    date_iso = dt.strftime('%Y-%m-%d')
                except (TypeError, ValueError):
                    date_display, time_display, date_iso = stamp or '', '', ''
                content = ' '.join((job.content or '').split())
                if keyword and keyword not in content.casefold() and keyword not in job.group_url.casefold():
                    continue
                if date_q and date_q.casefold() not in date_display.casefold() and date_q not in date_iso:
                    continue
                if time_q and time_q.casefold() not in time_display.casefold():
                    continue
                if group_q and group_q not in job.group_url.casefold():
                    continue
                matched.append((job, f'{date_display} {time_display}'.strip(), content))
            for job, stamp, content in matched:
                tree.insert('', 'end', iid=str(job.id), values=(
                    job.id, stamp, content[:220], job.group_url,
                    LABELS.get(job.status, job.status), job.permalink or ''))
            self._last_search_job_ids = [job.id for job, _stamp, _content in matched]
            footer.configure(text=f'Tìm thấy {len(matched)} bài trong tổng {len(jobs)} bản ghi. '
                                  'Thời điểm hiển thị ưu tiên thời điểm nhấn Đăng; nếu chưa nhấn thì dùng thời điểm tạo tác vụ.')

        def open_selected(_event=None):
            sel = tree.selection()
            if not sel:
                return
            job = self.store.get(int(sel[0]))
            url = job.permalink or job.group_url
            if url:
                webbrowser.open(url)

        tk.Button(filters, text='Tìm kiếm', command=run_search, bg='#6558F5', fg='white',
                  relief='flat', cursor='hand2', padx=16, pady=7,
                  font=('Segoe UI', 9, 'bold')).grid(row=4, column=0, sticky='w', padx=6, pady=(8, 0))
        tk.Button(filters, text='Xóa bộ lọc', command=lambda: [v.set('') for v in fields.values()],
                  bg='white', fg='#475569', relief='solid', bd=1,
                  cursor='hand2', padx=14, pady=7).grid(row=4, column=1, sticky='w', padx=6, pady=(8, 0))
        tree.bind('<Double-1>', open_selected)
        run_search()

    def _group_urls_from_text(self, raw: str) -> list[str]:
        urls = []
        for line_no, line in enumerate((raw or '').splitlines(), 1):
            if not line.strip():
                continue
            try:
                urls.append(clean_group(line))
            except ValueError as exc:
                raise ValueError(f'Dòng {line_no}: {exc}') from exc
        return urls

    def check_duplicate_groups(self):
        try:
            raw = self.groups_input.get('1.0', 'end-1c')
            urls = self._group_urls_from_text(raw)
        except ValueError as exc:
            messagebox.showerror('Link nhóm không hợp lệ', str(exc))
            return
        seen, duplicates, unique = set(), [], []
        for url in urls:
            if url in seen:
                duplicates.append(url)
            else:
                seen.add(url)
                unique.append(url)
        if not duplicates:
            messagebox.showinfo('Kiểm tra nhóm', f'Không có link trùng. Tổng cộng {len(unique)} nhóm hợp lệ.')
            return
        detail = '\n'.join(f'• {u}' for u in dict.fromkeys(duplicates))
        if messagebox.askyesno('Phát hiện link nhóm trùng',
                f'Có {len(duplicates)} dòng bị trùng ({len(set(duplicates))} link khác nhau):\n\n'
                f'{detail}\n\nBạn có muốn xóa các dòng trùng và chỉ giữ một link cho mỗi nhóm không?'):
            self.groups_input.delete('1.0', 'end')
            self.groups_input.insert('1.0', '\n'.join(unique))
            self.queue_save()
            self._refresh_group_list()
            messagebox.showinfo('Đã cập nhật', f'Đã giữ lại {len(unique)} nhóm không trùng.')

    def add_group_links(self):
        raw = simpledialog.askstring(
            'Thêm nhiều nhóm Facebook',
            'Dán link nhóm Facebook (mỗi link một dòng):',
            parent=self.root)
        if raw is None or not raw.strip():
            return
        try:
            incoming = self._group_urls_from_text(raw)
            existing = self._group_urls_from_text(self.groups_input.get('1.0', 'end-1c'))
        except ValueError as exc:
            messagebox.showerror('Link nhóm không hợp lệ', str(exc))
            return

        seen_existing = set(existing)
        seen_incoming = set()
        duplicates = []
        additions = []
        for url in incoming:
            if url in seen_existing or url in seen_incoming:
                duplicates.append(url)
            else:
                additions.append(url)
                seen_incoming.add(url)

        if duplicates:
            duplicate_lines = '\n'.join(f'• {u}' for u in dict.fromkeys(duplicates))
            remove = messagebox.askyesno(
                'Phát hiện link nhóm trùng',
                f'Có {len(duplicates)} link đã tồn tại hoặc bị lặp trong nội dung vừa dán:\n\n'
                f'{duplicate_lines}\n\nBạn có muốn xóa các link trùng khỏi danh sách và chỉ giữ một bản không?')
            if remove:
                # Normalize and de-duplicate the full saved list, retaining first occurrence.
                combined, seen = [], set()
                for url in existing + incoming:
                    if url not in seen:
                        combined.append(url)
                        seen.add(url)
                final_urls = combined
            else:
                final_urls = existing + additions
        else:
            final_urls = existing + additions

        self.groups_input.delete('1.0', 'end')
        self.groups_input.insert('1.0', '\n'.join(final_urls))
        self.queue_save()
        self._refresh_group_list()
        messagebox.showinfo('Quản lý nhóm',
            f'Đã cập nhật danh sách. Tổng cộng {len(final_urls)} nhóm; thêm mới {len(additions)} nhóm.')

    def delete_selected_group(self):
        selection = self.group_listbox.curselection()
        if not selection:
            messagebox.showinfo('Chọn nhóm', 'Hãy chọn một nhóm muốn xóa.')
            return
        url = self.group_urls_preview[selection[0]]
        if not messagebox.askyesno('Xóa nhóm', f'Bạn có chắc muốn xóa link nhóm này khỏi danh sách?\n\n{url}'):
            return
        urls = self._group_urls_from_text(self.groups_input.get('1.0', 'end-1c'))
        urls = [u for u in urls if u != url]
        self.groups_input.delete('1.0', 'end')
        self.groups_input.insert('1.0', '\n'.join(urls))
        self.queue_save()
        self._refresh_group_list()

    def _refresh_group_list(self):
        if not hasattr(self, 'group_listbox'):
            return
        try:
            raw = self.groups_input.get('1.0', 'end-1c')
            urls = self._group_urls_from_text(raw)
        except ValueError:
            # Keep the preview usable while the user is in the middle of editing.
            urls = []
            for line in raw.splitlines():
                try:
                    if line.strip():
                        urls.append(clean_group(line))
                except ValueError:
                    continue
        all_urls = list(dict.fromkeys(urls))
        query = ''
        if hasattr(self, 'group_search_var'):
            query = self.group_search_var.get().strip().casefold()
            if query == 'tìm link nhóm...':
                query = ''
        self.group_urls_preview = [u for u in all_urls if query in u.casefold()]
        if hasattr(self, 'group_count_label'):
            self.group_count_label.configure(text=f'{len(self.group_urls_preview)}/{len(all_urls)} nhóm')
        self.group_listbox.delete(0, 'end')
        for i, url in enumerate(self.group_urls_preview, 1):
            self.group_listbox.insert('end', f'{i:03d}.  {url}')
        if self.group_urls_preview:
            self.group_listbox.selection_set(0)
            self._show_group_preview()
        else:
            self.group_preview_name.configure(text='Chưa có đường dẫn nhóm')
            self.group_preview_url.configure(text='Vào mục Đăng bài hoặc bấm “Thêm nhóm” để nhập link Facebook.')

    def _show_group_preview(self, _event=None):
        selection = self.group_listbox.curselection()
        if not selection:
            return
        idx = selection[0]
        url = self.group_urls_preview[idx]
        slug = url.rstrip('/').split('/')[-1].replace('-', ' ').replace('_', ' ')
        name = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', slug).strip() or f'Nhóm Facebook {idx+1}'
        self.group_preview_name.configure(text=name.title())
        self.group_preview_url.configure(text=url, fg='#1877F2', cursor='hand2')
        self.group_preview_url.bind('<Button-1>', lambda _e, u=url: webbrowser.open(u))
        self.group_preview_status.configure(text='● Đã chọn nhóm · ảnh đại diện sẽ hiển thị khi Facebook cung cấp dữ liệu')

    def _open_preview_group(self):
        selection = self.group_listbox.curselection()
        if selection:
            webbrowser.open(self.group_urls_preview[selection[0]])

    def _build_library_schedule_tab(self):
        # Put the whole library/scheduler view inside a vertical scroll area so
        # lower buttons never get clipped on shorter windows or scaled displays.
        scroll_host = tk.Frame(self.plantab, bg=COLORS['bg'])
        scroll_host.pack(fill='both', expand=True)

        page_canvas = tk.Canvas(scroll_host, bg=COLORS['bg'], highlightthickness=0, bd=0)
        page_scrollbar = ttk.Scrollbar(scroll_host, orient='vertical', command=page_canvas.yview)
        page_canvas.configure(yscrollcommand=page_scrollbar.set)
        page_scrollbar.pack(side='right', fill='y')
        page_canvas.pack(side='left', fill='both', expand=True)

        outer = tk.Frame(page_canvas, bg=COLORS['bg'])
        outer_window = page_canvas.create_window((0, 0), window=outer, anchor='nw')

        def _update_scroll_region(_event=None):
            page_canvas.configure(scrollregion=page_canvas.bbox('all'))

        def _fit_outer_width(event):
            page_canvas.itemconfigure(outer_window, width=event.width)
            _update_scroll_region()

        outer.bind('<Configure>', _update_scroll_region)
        page_canvas.bind('<Configure>', _fit_outer_width)

        def _on_page_mousewheel(event):
            try:
                page_canvas.yview_scroll(int(-1 * (event.delta / 120)), 'units')
            except (AttributeError, tk.TclError):
                pass

        page_canvas.bind('<Enter>', lambda _e: page_canvas.bind_all('<MouseWheel>', _on_page_mousewheel))
        page_canvas.bind('<Leave>', lambda _e: page_canvas.unbind_all('<MouseWheel>'))

        outer.columnconfigure(0, weight=11)
        outer.columnconfigure(1, weight=10)
        outer.rowconfigure(0, weight=1)

        library = self._card(outer)
        library.grid(row=0, column=0, sticky='nsew', padx=(0, 8), pady=0)
        self._textlabel(library, '⑤ THƯ VIỆN BÀI ĐÃ LƯU', 12, True).pack(anchor='w')
        self._textlabel(library, 'Lưu nội dung, nhóm và bản sao ảnh để dùng lại vào ngày khác.',
                        9, color=COLORS['muted']).pack(anchor='w', pady=(3, 8))
        lframe = tk.Frame(library, bg='white')
        lframe.pack(fill='both', expand=True)
        self.library_tree = ttk.Treeview(lframe, columns=('id', 'title', 'groups', 'images'),
                                         show='headings', style='Jobs.Treeview', height=10)
        for name, label, width in [('id', 'ID', 45), ('title', 'Tên bài', 205),
                                   ('groups', 'Số nhóm', 75), ('images', 'Ảnh', 55)]:
            self.library_tree.heading(name, text=label)
            self.library_tree.column(name, width=width, minwidth=40, stretch=(name == 'title'))
        self.library_tree.pack(side='left', fill='both', expand=True)
        lscroll = ttk.Scrollbar(lframe, orient='vertical', command=self.library_tree.yview)
        lscroll.pack(side='right', fill='y')
        self.library_tree.configure(yscrollcommand=lscroll.set)
        self.library_tree.bind('<Double-Button-1>', lambda _: self.load_saved_post())
        lbuttons = tk.Frame(library, bg='white')
        lbuttons.pack(fill='x', pady=(8, 0))
        self._button(lbuttons, 'Lưu bài đang soạn', 'check', self.save_library_current,
                     COLORS['green'], small=True).pack(side='left', padx=(0, 5))
        self._button(lbuttons, 'Nạp để đăng lại', 'resume', self.load_saved_post,
                     COLORS['primary'], small=True).pack(side='left', padx=(0, 5))
        self._button(lbuttons, 'Xóa bài lưu', 'delete', self.delete_saved_post,
                     COLORS['red'], small=True).pack(side='left')
        self._button(lbuttons, 'Làm mới', 'refresh', self.refresh_library,
                     COLORS['orange'], small=True).pack(side='right')

        schedule = self._card(outer)
        schedule.grid(row=0, column=1, sticky='nsew', padx=(8, 0), pady=0)
        self._textlabel(schedule, '⑥ HẸN GIỜ ĐĂNG', 12, True).pack(anchor='w')
        self._textlabel(schedule, 'Hẹn bài đang soạn; đến giờ ứng dụng sẽ tự chạy trên Windows.',
                        9, color=COLORS['muted']).pack(anchor='w', pady=(3, 4))
        tk.Label(schedule, text='Lịch chỉ chạy khi ứng dụng Windows mở, máy không ngủ và Facebook đã đăng nhập. '
                 'Nếu ứng dụng đóng giữa lượt, lịch chuyển sang Cần kiểm tra để tránh đăng trùng.',
                 bg='white', fg=COLORS['muted'], font=('Segoe UI', 8), justify='left',
                 wraplength=480).pack(anchor='w', pady=(0, 5))
        form = tk.Frame(schedule, bg='white')
        form.pack(fill='x')
        self._textlabel(form, 'Tên lịch', 9, True).grid(row=0, column=0, sticky='w', padx=(0, 8), pady=3)
        self.schedule_title = tk.StringVar(value='Bài đăng theo lịch')
        ttk.Entry(form, textvariable=self.schedule_title, width=28).grid(row=0, column=1, sticky='ew', pady=3)
        future = datetime.now().astimezone() + timedelta(minutes=10)
        self.schedule_date = tk.StringVar(value=future.strftime('%Y-%m-%d'))
        self.schedule_time = tk.StringVar(value=future.strftime('%H:%M'))
        self._textlabel(form, 'Ngày đăng', 9, True).grid(row=1, column=0, sticky='w', padx=(0, 8), pady=4)
        date_row = tk.Frame(form, bg='white')
        date_row.grid(row=1, column=1, sticky='ew', pady=4)
        ttk.Entry(date_row, textvariable=self.schedule_date, width=13).pack(side='left', padx=(0, 6))
        self._button(date_row, 'Hôm nay', 'check', lambda: self._set_schedule_day(0),
                     COLORS['primary'], pad=6, small=True).pack(side='left', padx=(0, 4))
        self._button(date_row, 'Ngày mai', 'refresh', lambda: self._set_schedule_day(1),
                     COLORS['purple'], pad=6, small=True).pack(side='left')
        self._textlabel(form, 'Giờ đăng', 9, True).grid(row=2, column=0, sticky='w', padx=(0, 8), pady=4)
        time_row = tk.Frame(form, bg='white')
        time_row.grid(row=2, column=1, sticky='ew', pady=4)
        ttk.Entry(time_row, textvariable=self.schedule_time, width=9).pack(side='left', padx=(0, 6))
        self._button(time_row, '+10 phút', 'refresh', lambda: self._set_schedule_time(10),
                     COLORS['orange'], pad=6, small=True).pack(side='left', padx=(0, 4))
        self._button(time_row, '+1 giờ', 'refresh', lambda: self._set_schedule_time(60),
                     COLORS['orange'], pad=6, small=True).pack(side='left')
        form.columnconfigure(1, weight=1)
        self._button(schedule, 'TẠO LỊCH ĐĂNG BÀI', 'send', self.schedule_current,
                     COLORS['green'], small=False).pack(fill='x', pady=(9, 8))
        sframe = tk.Frame(schedule, bg='white')
        sframe.pack(fill='both', expand=True)
        self.schedule_tree = ttk.Treeview(sframe, columns=('title', 'when', 'state'),
                                          show='headings', style='Jobs.Treeview', height=7)
        for name, label, width in [('title', 'Tên lịch', 145), ('when', 'Ngày giờ', 135), ('state', 'Trạng thái', 110)]:
            self.schedule_tree.heading(name, text=label)
            self.schedule_tree.column(name, width=width, minwidth=65, stretch=(name == 'title'))
        self.schedule_tree.pack(side='left', fill='both', expand=True)
        sscroll = ttk.Scrollbar(sframe, orient='vertical', command=self.schedule_tree.yview)
        sscroll.pack(side='right', fill='y')
        self.schedule_tree.configure(yscrollcommand=sscroll.set)
        self.schedule_tree.bind('<Double-Button-1>', lambda _: self.load_selected_schedule())
        sbuttons = tk.Frame(schedule, bg='white')
        sbuttons.pack(fill='x', pady=(9, 0))
        for col in range(2):
            sbuttons.columnconfigure(col, weight=1, uniform='schedule_actions')
        self._button(sbuttons, 'Nạp lịch để sửa', 'resume', self.load_selected_schedule,
                     COLORS['primary'], pad=7, small=True).grid(row=0, column=0, sticky='ew', padx=(0, 4), pady=(0, 5))
        self._button(sbuttons, 'Đổi ngày / giờ', 'refresh', self.edit_selected_schedule_time,
                     COLORS['orange'], pad=7, small=True).grid(row=0, column=1, sticky='ew', padx=(4, 0), pady=(0, 5))
        self._button(sbuttons, 'Hủy lịch', 'delete', self.cancel_selected_schedule,
                     COLORS['red'], pad=7, small=True).grid(row=1, column=0, sticky='ew', padx=(0, 4))
        self._button(sbuttons, 'Làm mới danh sách', 'refresh', self.refresh_schedules,
                     COLORS['purple'], pad=7, small=True).grid(row=1, column=1, sticky='ew', padx=(4, 0))

    def _restore_draft(self):
        data = load_draft()
        self.content_input.insert('1.0', data.get('content', ''))
        self.groups_input.insert('1.0', data.get('groups', ''))
        if hasattr(self, 'group_listbox'):
            self._refresh_group_list()
        self.images = [str(p) for p in data.get('images', []) if Path(p).is_file()]
        self._display_photos()

    def _display_photos(self):
        for child in self.photo_preview.winfo_children():
            child.destroy()
        self._thumbnail_refs = []
        self.selected_image_indices = {i for i in self.selected_image_indices
                                       if 0 <= i < len(self.images)}
        self.photo_label.configure(text=f'{len(self.images)} ảnh đã chọn'
                                   + (' · bấm ảnh để chọn/xóa' if self.images else ''))
        if not self.images:
            tk.Label(self.photo_preview, text='Chưa chọn ảnh. Bấm “Thêm ảnh” để xem ảnh xem trước tại đây.',
                     bg='#f9fbff', fg=COLORS['muted'], font=('Segoe UI', 9),
                     padx=10, pady=18, wraplength=500, justify='left').grid(
                         row=0, column=0, sticky='w')
            self.photo_canvas.configure(scrollregion=self.photo_canvas.bbox('all'))
            return
        try:
            from PIL import Image, ImageTk, ImageOps, ImageDraw
            pillow_ok = True
        except ImportError:
            pillow_ok = False
        columns = 4
        for i, path in enumerate(self.images):
            selected = i in self.selected_image_indices
            border = '#2878d0' if selected else '#d7e0ed'
            card = tk.Frame(self.photo_preview, bg='#dceaff' if selected else 'white',
                            highlightthickness=2 if selected else 1,
                            highlightbackground=border, padx=4, pady=4, cursor='hand2')
            card.grid(row=i // columns, column=i % columns, padx=5, pady=5, sticky='nsew')
            try:
                if not pillow_ok:
                    raise ImportError('Pillow chưa được cài')
                with Image.open(path) as im:
                    im = ImageOps.exif_transpose(im).convert('RGB')
                    im.thumbnail((112, 92))
                    background = Image.new('RGB', (116, 96), 'white')
                    background.paste(im, ((116 - im.width) // 2, (96 - im.height) // 2))
                    thumb = ImageTk.PhotoImage(background)
                self._thumbnail_refs.append(thumb)
                image_label = tk.Label(card, image=thumb, bg=card.cget('bg'), cursor='hand2')
                image_label.pack()
            except Exception:
                image_label = tk.Label(card, text='Không xem được ảnh', width=15, height=5,
                                       bg=card.cget('bg'), fg=COLORS['muted'],
                                       font=('Segoe UI', 8), cursor='hand2')
                image_label.pack()
            name = Path(path).name
            if len(name) > 20:
                name = name[:17] + '...'
            name_label = tk.Label(card, text=name, bg=card.cget('bg'), fg=COLORS['ink'],
                                  font=('Segoe UI', 8), wraplength=116, cursor='hand2')
            name_label.pack(fill='x', pady=(3, 0))
            mark = tk.Label(card, text='✓ ĐÃ CHỌN' if selected else 'Bấm để chọn',
                            bg=card.cget('bg'), fg='#1766b1' if selected else COLORS['muted'],
                            font=('Segoe UI', 7, 'bold' if selected else 'normal'), cursor='hand2')
            mark.pack(fill='x')
            for widget in (card, image_label, name_label, mark):
                widget.bind('<Button-1>', lambda _e, idx=i: self._toggle_image_selection(idx))
        for col in range(columns):
            self.photo_preview.grid_columnconfigure(col, weight=1)
        self.photo_canvas.configure(scrollregion=self.photo_canvas.bbox('all'))

    def _toggle_image_selection(self, index):
        if index in self.selected_image_indices:
            self.selected_image_indices.remove(index)
        else:
            self.selected_image_indices.add(index)
        self._display_photos()

    def queue_save(self):
        if self._save_after:
            self.root.after_cancel(self._save_after)
        self._save_after = self.root.after(1100, self.save)

    def save(self):
        self._save_after = None
        try:
            save_draft(self.content_input.get('1.0', 'end-1c'),
                       self.groups_input.get('1.0', 'end-1c'), self.images)
        except Exception as exc:
            self._log('Không lưu được bản nháp: ' + str(exc))

    def add_images(self):
        chosen = filedialog.askopenfilenames(title='Chọn ảnh bài đăng',
            filetypes=[('Ảnh', '*.jpg *.jpeg *.png *.webp')])
        if chosen:
            self.images = list(dict.fromkeys(self.images + list(chosen)))
            self.selected_image_indices.clear()
            if len(self.images) > MAX_IMAGES:
                messagebox.showwarning('Giới hạn', f'Chỉ nhận tối đa {MAX_IMAGES} ảnh.')
                self.images = self.images[:MAX_IMAGES]
            self._display_photos()
            self.queue_save()

    def remove_images(self):
        selection = set(self.selected_image_indices)
        if not selection:
            messagebox.showinfo('Chưa chọn ảnh', 'Bấm vào ảnh muốn xóa trước, ảnh được chọn sẽ có viền xanh.')
            return
        self.images = [p for i, p in enumerate(self.images) if i not in selection]
        self.selected_image_indices.clear()
        self._display_photos()
        self.queue_save()

    def export_groups_txt(self):
        """Export normalized Facebook group links as a UTF-8 text file."""
        try:
            raw = self.groups_input.get('1.0', 'end-1c')
            groups = list(dict.fromkeys(self._group_urls_from_text(raw)))
        except ValueError as exc:
            messagebox.showerror('Danh sách nhóm không hợp lệ', str(exc))
            return
        if not groups:
            messagebox.showinfo('Xuất danh sách nhóm', 'Chưa có nhóm nào để xuất.')
            return
        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            parent=self.root, title='Xuất danh sách link nhóm',
            defaultextension='.txt', initialfile=f'postflow_nhom_{datetime.now():%Y%m%d_%H%M}.txt',
            filetypes=[('Text UTF-8', '*.txt'), ('CSV', '*.csv')])
        if not filename:
            return
        try:
            Path(filename).write_text('\n'.join(groups) + '\n', encoding='utf-8-sig')
            self.store.add_activity('Xuất dữ liệu', f'Đã xuất {len(groups)} link nhóm: {filename}')
            messagebox.showinfo('Xuất danh sách thành công', f'Đã xuất {len(groups)} nhóm.\n\n{filename}')
        except Exception as exc:
            messagebox.showerror('Không thể xuất danh sách', str(exc))

    def export_jobs_csv(self, job_ids=None):
        """Export all posting records or a filtered search result to Excel-friendly UTF-8 CSV."""
        jobs = self.store.all(limit=1000000)
        if job_ids is not None:
            wanted = set(job_ids)
            jobs = [job for job in jobs if job.id in wanted]
        if not jobs:
            messagebox.showinfo('Xuất lịch sử', 'Không có dữ liệu để xuất.')
            return
        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            parent=self.root, title='Lưu lịch sử PostFlow',
            defaultextension='.csv', initialfile=f'postflow_lich_su_{datetime.now():%Y%m%d_%H%M}.csv',
            filetypes=[('CSV UTF-8', '*.csv')])
        if not filename:
            return
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as handle:
                writer = csv.writer(handle)
                writer.writerow(['ID', 'Ngày giờ tạo', 'Thời điểm nhấn Đăng', 'Ngày', 'Giờ',
                                 'Nội dung', 'Link nhóm', 'Trạng thái', 'Ghi chú',
                                 'Link bài viết', 'Ảnh', 'Đường dẫn bằng chứng'])
                for job in jobs:
                    stamp = job.sent_intent_at or job.created_at or ''
                    try:
                        dt = datetime.fromisoformat(stamp).astimezone()
                        day, clock = dt.strftime('%d/%m/%Y'), dt.strftime('%H:%M:%S')
                    except (TypeError, ValueError):
                        day, clock = '', ''
                    writer.writerow([job.id, job.created_at, job.sent_intent_at or '', day, clock,
                                     job.content, job.group_url, LABELS.get(job.status, job.status),
                                     job.note, job.permalink, ' | '.join(job.images), job.evidence_path])
            self.store.add_activity('Xuất dữ liệu', f'Đã xuất {len(jobs)} bản ghi CSV: {filename}')
            messagebox.showinfo('Xuất dữ liệu thành công', f'Đã xuất {len(jobs)} bản ghi.\n\n{filename}')
        except Exception as exc:
            messagebox.showerror('Không thể xuất CSV', str(exc))

    def backup_data(self):
        """Create a consistent SQLite backup plus draft and app settings; omit browser credentials."""
        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            parent=self.root, title='Tạo bản sao lưu PostFlow',
            defaultextension='.zip', initialfile=f'postflow_backup_{datetime.now():%Y%m%d_%H%M}.zip',
            filetypes=[('ZIP backup', '*.zip')])
        if not filename:
            return
        temp_db = HOME / f'backup_tmp_{uuid.uuid4().hex}.sqlite3'
        try:
            source = sqlite3.connect(DB_PATH, timeout=15)
            target = sqlite3.connect(temp_db)
            try:
                source.backup(target)
            finally:
                target.close()
                source.close()
            with zipfile.ZipFile(filename, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                archive.write(temp_db, 'cong_viec.sqlite3')
                if DRAFT_PATH.exists():
                    archive.write(DRAFT_PATH, 'ban_nhap.json')
                manifest = {
                    'application': 'PostFlow Facebook Publisher',
                    'backup_created_at': now(),
                    'database': 'cong_viec.sqlite3',
                    'includes': ['database', 'draft_if_available'],
                    'excludes': ['Facebook browser profile and login session'],
                    'restore_note': 'Đóng PostFlow, sao lưu file dữ liệu hiện tại, sau đó thay cong_viec.sqlite3 trong thư mục dữ liệu bằng bản trong ZIP.'
                }
                archive.writestr('backup_info.json', json.dumps(manifest, ensure_ascii=False, indent=2))
            self.store.add_activity('Sao lưu', f'Đã tạo bản sao lưu: {filename}')
            messagebox.showinfo('Sao lưu thành công', f'Đã tạo bản sao lưu dữ liệu.\n\n{filename}\n\nPhiên đăng nhập Facebook không được đưa vào file sao lưu.')
        except Exception as exc:
            messagebox.showerror('Sao lưu thất bại', str(exc))
        finally:
            try:
                temp_db.unlink(missing_ok=True)
            except OSError:
                pass

    def open_data_folder(self):
        """Open the local PostFlow data folder using the operating system file manager."""
        try:
            HOME.mkdir(parents=True, exist_ok=True)
            if os.name == 'nt':
                os.startfile(str(HOME))
            elif sys.platform == 'darwin':
                import subprocess
                subprocess.Popen(['open', str(HOME)])
            else:
                import subprocess
                subprocess.Popen(['xdg-open', str(HOME)])
        except Exception as exc:
            messagebox.showerror('Không mở được thư mục', str(exc))

    def refresh_activity(self):
        tree = getattr(self, 'activity_tree', None)
        if tree is None:
            return
        try:
            for item in tree.get_children():
                tree.delete(item)
            for event in self.store.list_activity():
                stamp = event.get('created_at', '')
                try:
                    stamp = datetime.fromisoformat(stamp).astimezone().strftime('%d/%m/%Y %H:%M:%S')
                except Exception:
                    pass
                category = event.get('category', 'Hoạt động')
                message = event.get('message', '')
                tag = 'error' if any(word in (category + ' ' + message).lower()
                                     for word in ('lỗi', 'thất bại', 'không thể', 'error')) else 'normal'
                tree.insert('', 'end', values=(stamp, category, message), tags=(tag,))
        except (tk.TclError, sqlite3.Error):
            pass

    def clear_activity_log(self):
        if not messagebox.askyesno('Xóa nhật ký hoạt động',
                                   'Bạn có chắc muốn xóa toàn bộ nhật ký hoạt động của ứng dụng? '
                                   'Lịch sử đăng bài sẽ không bị xóa.'):
            return
        self.store.clear_activity()
        self.refresh_activity()
        self._log('Đã xóa nhật ký hoạt động ứng dụng.')

    def _log(self, message: str):
        self.console.configure(state='normal')
        self.console.insert('end', f'[{time.strftime("%H:%M:%S")}] {message}\n')
        self.console.see('end')
        self.console.configure(state='disabled')
        self.status_text.set(message[:155])
        try:
            lower = message.lower()
            category = ('Lỗi' if any(k in lower for k in ('lỗi', 'thất bại', 'không thể', 'error'))
                        else 'Đăng bài' if any(k in lower for k in ('đăng', 'nhóm', 'facebook'))
                        else 'Lịch hẹn' if any(k in lower for k in ('lịch', 'đến hạn', 'hẹn'))
                        else 'Ứng dụng')
            self.store.add_activity(category, message)
            self.refresh_activity()
        except (AttributeError, sqlite3.Error, tk.TclError):
            pass

    def emit(self, message: str):
        self.ui_events.put(('log', message))

    def _handle_ui(self):
        try:
            while True:
                kind, value = self.ui_events.get_nowait()
                if kind == 'log':
                    self._log(str(value))
                elif kind == 'refresh':
                    self.refresh_jobs()
                elif kind == 'refresh_library':
                    self.refresh_library()
                elif kind == 'refresh_schedules':
                    self.refresh_schedules()
                elif kind == 'refresh_photos':
                    self._display_photos()
                elif kind == 'done':
                    self.worker_running = False
                    self.refresh_jobs()
                    self._log(str(value))
        except queue.Empty:
            pass
        self.root.after(150, self._handle_ui)

    def refresh_jobs(self):
        chosen = self.tree.selection()
        current = chosen[0] if chosen else None
        self.tree.delete(*self.tree.get_children())
        jobs = self.store.all(limit=1000000)
        for job in jobs:
            if job.status in (CONFIRMED, OBSERVED, ACKNOWLEDGED):
                tag = 'done'
            elif job.status == FAILED:
                tag = 'issue'
            elif job.status in (REVIEW, PENDING):
                tag = 'review'
            else:
                tag = 'normal'
            # sent_intent_at được lưu ngay trước khi ứng dụng nhấn nút Đăng.
            # Với bài chưa từng nhấn Đăng, hiển thị thời điểm tạo công việc để không để trống.
            posted_time = job.sent_intent_at or job.created_at
            try:
                posted_dt = datetime.fromisoformat(posted_time)
                posted_time = posted_dt.astimezone().strftime('%d/%m/%Y %H:%M')
            except (TypeError, ValueError):
                posted_time = posted_time or '—'
            self.tree.insert('', 'end', iid=str(job.id),
                values=(job.id, job.group_url, posted_time,
                        LABELS.get(job.status, job.status), job.note),
                tags=(tag,))
        sent_count = sum(j.status in (OBSERVED, ACKNOWLEDGED, CONFIRMED) for j in jobs)
        waiting_count = sum(j.status == PENDING for j in jobs)
        unclear_count = sum(j.status == REVIEW for j in jobs)
        failed_count = sum(j.status == FAILED for j in jobs)
        queue_count = sum(j.status == QUEUED for j in jobs)
        if hasattr(self, 'dashboard_values'):
            self.dashboard_values['total'].configure(text=str(len(jobs)))
            self.dashboard_values['success'].configure(text=str(sum(j.status in (CONFIRMED, OBSERVED, ACKNOWLEDGED) for j in jobs)))
            self.dashboard_values['review'].configure(text=str(waiting_count + unclear_count + queue_count + sum(j.status in (PREPARING, SUBMITTING) for j in jobs)))
            self.dashboard_values['failed'].configure(text=str(failed_count))
        if hasattr(self, 'dashboard_period_summary'):
            current = datetime.now().astimezone()
            today_count = month_count = year_count = 0
            for job in jobs:
                stamp = job.sent_intent_at or job.created_at
                try:
                    dt = datetime.fromisoformat(stamp).astimezone()
                except (TypeError, ValueError):
                    continue
                today_count += (dt.date() == current.date())
                month_count += (dt.year == current.year and dt.month == current.month)
                year_count += (dt.year == current.year)
            try:
                saved_groups = self._group_urls_from_text(self.groups_input.get('1.0', 'end-1c'))
                group_count = len(set(saved_groups))
            except Exception:
                group_count = 0
            self.dashboard_period_summary.configure(
                text=f'Nhóm đang quản lý: {group_count}  |  Công việc hôm nay: {today_count}  |  '
                     f'Tháng {current.month:02d}/{current.year}: {month_count}  |  Năm {current.year}: {year_count}  |  '
                     f'Lịch hẹn: {len([x for x in self.store.list_schedules(limit=100000) if x.get("status") == "scheduled"])}')
        if hasattr(self, 'dashboard_refresh_chart'):
            try:
                self.dashboard_refresh_chart()
            except tk.TclError:
                pass
        if hasattr(self, 'dashboard_recent'):
            for child in self.dashboard_recent.winfo_children():
                child.destroy()
            if not jobs:
                tk.Label(self.dashboard_recent, text='Chưa có hoạt động. Hãy tạo bài đăng để bắt đầu.',
                         bg='white', fg='#64748B', font=('Segoe UI', 9)).pack(anchor='w', pady=8)
            else:
                for job in jobs[:4]:
                    state = LABELS.get(job.status, job.status)
                    row = tk.Frame(self.dashboard_recent, bg='white')
                    row.pack(fill='x', pady=5)
                    icon_color = '#0F9F8F' if job.status == CONFIRMED else ('#E54868' if job.status == FAILED else '#6558F5')
                    tk.Label(row, text='▤', bg='#EEF2FF', fg=icon_color,
                             font=('Segoe UI Symbol', 12, 'bold'), padx=8, pady=5).pack(side='left', padx=(0, 10))
                    details = tk.Frame(row, bg='white')
                    details.pack(side='left', fill='x', expand=True)
                    snippet = ' '.join((job.content or '').split())
                    if len(snippet) > 100:
                        snippet = snippet[:97] + '…'
                    tk.Label(details, text=f'#{job.id}  {snippet or "Chưa có nội dung"}', bg='white', fg='#172554',
                             font=('Segoe UI', 9, 'bold'), anchor='w').pack(anchor='w', fill='x')
                    tk.Label(details, text=job.group_url, bg='white', fg='#94A3B8',
                             font=('Segoe UI', 8), anchor='w').pack(anchor='w', fill='x', pady=(2, 0))
                    tk.Label(row, text=state, bg='#E6FFFB' if job.status == CONFIRMED else ('#FFF1F2' if job.status == FAILED else '#EEF2FF'),
                             fg=icon_color, font=('Segoe UI', 8, 'bold'), padx=8, pady=4).pack(side='right', padx=(8, 0))
                    tk.Frame(self.dashboard_recent, bg='#E2E8F0', height=1).pack(fill='x', pady=(3, 0))
        self.summary_text.set(
            f'Đã thấy/FB phản hồi: {sent_count}  |  Chờ duyệt: {waiting_count}  |  '
            f'Chưa rõ: {unclear_count}  |  Bỏ qua lỗi: {failed_count}  |  Chưa chạy: {queue_count}')
        if current and self.tree.exists(current):
            self.tree.selection_set(current)

    def _selected(self):
        selection = self.tree.selection()
        if not selection:
            messagebox.showinfo('Chọn bài', 'Hãy chọn một dòng trong bảng lịch sử trước.')
            return None
        return self.store.get(int(selection[0]))

    def _set_editor_payload(self, content: str, groups_text: str, images: list[str], *, source: str, allow_repost: bool):
        missing = [p for p in images if not Path(p).is_file()]
        usable = [str(Path(p)) for p in images if Path(p).is_file()]
        self.content_input.delete('1.0', 'end')
        self.content_input.insert('1.0', content or '')
        self.groups_input.delete('1.0', 'end')
        self.groups_input.insert('1.0', groups_text or '')
        if hasattr(self, 'group_listbox'):
            self._refresh_group_list()
        self.images = usable[:MAX_IMAGES]
        self.repeat_key_next = f'{source}-{uuid.uuid4().hex}' if allow_repost else None
        self._display_photos()
        self.queue_save()
        self.tabs.select(self.worktab)
        if missing:
            messagebox.showwarning('Một số ảnh không còn',
                f'{len(missing)} ảnh trong bài cũ không tìm thấy trên máy. Hãy thêm lại ảnh trước khi đăng.')
        self._log('Đã nạp nội dung cũ vào khung soạn. Kiểm tra nhóm và ảnh trước khi đăng lại.')

    def reuse_history_post(self):
        job = self._selected()
        if not job:
            return
        if not messagebox.askyesno('Dùng lại bài cũ',
                'Nạp nội dung và các nhóm thuộc cùng lượt đăng vào khung soạn?\n'
                'Khi bạn nhấn Đăng, đây được coi là yêu cầu đăng lại có chủ đích.'):
            return
        related = []
        try:
            selected_time = datetime.fromisoformat(job.created_at)
        except (TypeError, ValueError):
            selected_time = None
        for candidate in self.store.all(limit=2000):
            if candidate.content != job.content or candidate.images != job.images:
                continue
            try:
                candidate_time = datetime.fromisoformat(candidate.created_at)
            except (TypeError, ValueError):
                continue
            if selected_time is not None and abs((candidate_time - selected_time).total_seconds()) > 300:
                continue
            related.append(candidate)
        related.sort(key=lambda item: item.id)
        groups = list(dict.fromkeys(item.group_url for item in related)) or [job.group_url]
        self._set_editor_payload(job.content, '\\n'.join(groups), job.images,
                                 source='history', allow_repost=True)

    def refresh_library(self):
        if not hasattr(self, 'library_tree'):
            return
        selected = self.library_tree.selection()
        old = selected[0] if selected else None
        self.library_tree.delete(*self.library_tree.get_children())
        for item in self.store.list_library_posts():
            try:
                group_count = len(clean_groups(item['groups_text']))
            except ValueError:
                group_count = 0
            self.library_tree.insert('', 'end', iid=str(item['id']),
                values=(item['id'], item['title'], group_count, item['image_count']))
        if old and self.library_tree.exists(old):
            self.library_tree.selection_set(old)

    def save_library_current(self):
        title = simpledialog.askstring('Lưu bài mẫu', 'Đặt tên cho nội dung để dùng lại:', parent=self.root)
        if title is None:
            return
        try:
            post_id = self.store.save_library_post(
                title, self.content_input.get('1.0', 'end-1c'),
                self.groups_input.get('1.0', 'end-1c'), self.images)
        except (ValueError, OSError, shutil.Error) as exc:
            messagebox.showerror('Không lưu được bài', str(exc))
            return
        self.refresh_library()
        self._log(f'Đã lưu bài mẫu #{post_id}: {title.strip()}. Ảnh được sao chép vào thư viện của ứng dụng.')

    def _selected_library_id(self):
        selection = self.library_tree.selection()
        if not selection:
            messagebox.showinfo('Chọn bài mẫu', 'Hãy chọn một bài trong thư viện trước.')
            return None
        return int(selection[0])

    def load_saved_post(self):
        post_id = self._selected_library_id()
        if post_id is None:
            return
        post = self.store.get_library_post(post_id)
        if not messagebox.askyesno('Nạp bài mẫu',
                f'Nạp “{post["title"]}” vào khung soạn? Khi nhấn Đăng, ứng dụng cho phép đăng lại có chủ đích.'):
            return
        self._set_editor_payload(post['content'], post['groups_text'], post['images'],
                                 source='library', allow_repost=True)

    def delete_saved_post(self):
        post_id = self._selected_library_id()
        if post_id is None:
            return
        post = self.store.get_library_post(post_id)
        if messagebox.askyesno('Xóa bài mẫu', f'Xóa “{post["title"]}” khỏi thư viện?'):
            self.store.delete_library_post(post_id)
            self.refresh_library()
            self._log(f'Đã xóa bài mẫu #{post_id}.')

    def refresh_schedules(self):
        if not hasattr(self, 'schedule_tree'):
            return
        selected = self.schedule_tree.selection()
        old = selected[0] if selected else None
        self.schedule_tree.delete(*self.schedule_tree.get_children())
        labels = {
            'scheduled': 'Đã hẹn', 'running': 'Đang chạy', 'finished': 'Đã chạy',
            'cancelled': 'Đã hủy', 'error': 'Lỗi', 'needs_review': 'Cần kiểm tra',
            'stopped': 'Đã dừng',
        }
        for item in self.store.list_schedules():
            when = str(item.get('scheduled_at', '')).replace('T', ' ')[:16]
            self.schedule_tree.insert('', 'end', iid=item['id'],
                values=(item['title'], when, labels.get(item['status'], item['status'])))
        if old and self.schedule_tree.exists(old):
            self.schedule_tree.selection_set(old)

    def _selected_schedule(self):
        selection = self.schedule_tree.selection()
        if not selection:
            messagebox.showinfo('Chọn lịch', 'Hãy chọn một lịch đăng trước.')
            return None
        return self.store.get_schedule(selection[0])

    def _set_schedule_day(self, offset: int):
        """Quick-select today/tomorrow while preserving the chosen clock time."""
        day = datetime.now().astimezone().date() + timedelta(days=max(0, int(offset)))
        self.schedule_date.set(day.strftime('%Y-%m-%d'))
        self._log(f'Đã chọn ngày hẹn: {day.strftime("%d/%m/%Y")}.')

    def _set_schedule_time(self, minutes: int):
        """Set a convenient future time and roll over to tomorrow when needed."""
        base = datetime.now().astimezone()
        try:
            selected_day = datetime.strptime(self.schedule_date.get().strip(), '%Y-%m-%d').date()
            selected_clock = datetime.strptime(self.schedule_time.get().strip(), '%H:%M').time()
            base = base.replace(year=selected_day.year, month=selected_day.month, day=selected_day.day,
                                hour=selected_clock.hour, minute=selected_clock.minute, second=0, microsecond=0)
        except ValueError:
            pass
        target = base + timedelta(minutes=max(1, int(minutes)))
        self.schedule_date.set(target.strftime('%Y-%m-%d'))
        self.schedule_time.set(target.strftime('%H:%M'))

    def schedule_current(self):
        try:
            when = parse_local_datetime(
                f'{self.schedule_date.get().strip()} {self.schedule_time.get().strip()}')
            if when <= datetime.now().astimezone() + timedelta(seconds=10):
                raise ValueError('Giờ hẹn phải ở tương lai. Bạn có thể chọn Hôm nay (nếu còn giờ) hoặc Ngày mai.')
            title = self.schedule_title.get().strip() or 'Bài đăng theo lịch'
            schedule_id = self.store.add_schedule(
                title, self.content_input.get('1.0', 'end-1c'),
                self.groups_input.get('1.0', 'end-1c'), self.images,
                when.isoformat(timespec='seconds'), int(self.delay_seconds.get()))
        except (ValueError, OSError, shutil.Error) as exc:
            messagebox.showerror('Không tạo được lịch', str(exc))
            return
        self.refresh_schedules()
        self._log(f'Đã hẹn “{title}” lúc {when.strftime("%Y-%m-%d %H:%M")}. Ứng dụng Windows cần mở đến giờ đó.')
        messagebox.showinfo('Đã tạo lịch',
            f'Đã lưu lịch “{title}” vào {when.strftime("%d/%m/%Y %H:%M")}.\n'
            'Giữ ứng dụng mở, máy tính không ngủ và Facebook đã đăng nhập.')

    def cancel_selected_schedule(self):
        item = self._selected_schedule()
        if not item:
            return
        if item['status'] != 'scheduled':
            messagebox.showwarning('Không thể hủy', 'Chỉ có thể hủy lịch đang ở trạng thái “Đã hẹn”.')
            return
        if messagebox.askyesno('Hủy lịch', f'Hủy lịch “{item["title"]}” lúc {item["scheduled_at"]}?'):
            if self.store.cancel_schedule(item['id']):
                self.refresh_schedules()
                self._log(f'Đã hủy lịch: {item["title"]}.')

    def edit_selected_schedule_time(self):
        item = self._selected_schedule()
        if not item:
            return
        if item['status'] != 'scheduled':
            messagebox.showwarning('Không thể đổi giờ', 'Chỉ có thể chỉnh lịch đang ở trạng thái “Đã hẹn”.')
            return
        current = datetime.fromisoformat(item['scheduled_at']).astimezone().strftime('%Y-%m-%d %H:%M')
        value = simpledialog.askstring('Đổi giờ hẹn đăng',
            'Nhập ngày giờ mới theo định dạng YYYY-MM-DD HH:MM:', initialvalue=current, parent=self.root)
        if value is None:
            return
        try:
            when = parse_local_datetime(value.strip())
            if self.store.reschedule(item['id'], when.isoformat(timespec='seconds')):
                self.refresh_schedules()
                self._log(f'Đã đổi giờ lịch “{item["title"]}” sang {when:%Y-%m-%d %H:%M}.')
            else:
                messagebox.showwarning('Không đổi được', 'Lịch không còn ở trạng thái Đã hẹn.')
        except ValueError as exc:
            messagebox.showerror('Ngày giờ không hợp lệ', str(exc))

    def load_selected_schedule(self):
        item = self._selected_schedule()
        if not item:
            return
        if not messagebox.askyesno('Nạp lịch',
                f'Nạp “{item["title"]}” vào khung soạn? Đây là thao tác nạp để chỉnh sửa/đăng lại, không thay đổi lịch cũ.'):
            return
        self._set_editor_payload(item['content'], item['groups_text'], item['images'],
                                 source='schedule-edit', allow_repost=True)

    def _check_due_schedules(self):
        """Poll every second; due schedules launch as soon as the publisher is idle."""
        try:
            now_local = datetime.now().astimezone()
            if not self.worker_running:
                for item in self.store.list_schedules():
                    if item.get('status') != 'scheduled':
                        continue
                    try:
                        due = parse_local_datetime(item['scheduled_at']) <= now_local
                    except (TypeError, ValueError) as exc:
                        self.store.update_schedule(item['id'], 'error', error=f'Ngày giờ trong lịch không hợp lệ: {exc}')
                        self.refresh_schedules()
                        continue
                    if due and self.store.claim_schedule(item['id']):
                        self._log(f'Đã đến giờ lịch “{item["title"]}”; đang khởi chạy...')
                        self._launch_due_schedule(self.store.get_schedule(item['id']))
                        break
        except Exception as exc:
            self._log(f'Lỗi kiểm tra lịch: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}')
        finally:
            try:
                self.root.after(1000, self._check_due_schedules)
            except tk.TclError:
                pass

    def _launch_due_schedule(self, item: dict):
        try:
            groups = clean_groups(item['groups_text'])
            images = validate_images(item['images'])
            ids, _duplicates = self.store.add_jobs(
                item['content'], images, groups, repeat_key=f'schedule-{item["id"]}')
            if not ids:
                raise RuntimeError('Không tạo được công việc từ lịch này.')
            self.store.update_schedule(item['id'], job_ids=ids)
            self.current_ids = ids
            self.refresh_jobs()
            self.refresh_schedules()
            self._log(f'Đến giờ hẹn “{item["title"]}”: bắt đầu đăng tới {len(ids)} nhóm.')
            self._run_ids(ids, delay_override=int(item['delay_seconds']), schedule_id=item['id'])
        except Exception as exc:
            self.store.update_schedule(item['id'], 'error', error=f'{type(exc).__name__}: {str(exc).splitlines()[0][:180]}')
            self.refresh_schedules()
            self._log(f'Không chạy được lịch “{item["title"]}”: {type(exc).__name__}: {exc}')

    def view_history_details(self):
        """Hiển thị toàn bộ nhật ký của dòng đang chọn, không cắt ngắn nội dung."""
        job = self._selected()
        if not job:
            return

        detail = tk.Toplevel(self.root)
        detail.title(f"Chi tiết lịch sử bài đăng #{job.id}")
        detail.geometry("900x700")
        detail.minsize(650, 480)
        detail.transient(self.root)

        header = tk.Frame(detail, padx=14, pady=10)
        header.pack(fill="x")
        tk.Label(header, text=f"CHI TIẾT LỊCH SỬ  ·  #{job.id}",
                 font=("Segoe UI", 14, "bold")).pack(anchor="w")
        tk.Label(header, text=LABELS.get(job.status, job.status),
                 font=("Segoe UI", 10, "bold"), fg="#173763").pack(anchor="w", pady=(4, 0))

        body = ScrolledText(detail, wrap="word", font=("Segoe UI", 10),
                            padx=12, pady=10, undo=False)
        body.pack(fill="both", expand=True, padx=14, pady=(0, 10))

        def fmt_time(value):
            if not value:
                return "Chưa có"
            try:
                return datetime.fromisoformat(value).astimezone().strftime("%d/%m/%Y %H:%M:%S")
            except (TypeError, ValueError):
                return str(value)

        images_text = "\\n".join(f"  {i}. {path}" for i, path in enumerate(job.images, 1)) or "Không có ảnh"
        detail_text = (
            f"ID công việc: {job.id}\\n"
            f"Nhóm Facebook: {job.group_url}\\n"
            f"Trạng thái: {LABELS.get(job.status, job.status)} ({job.status})\\n"
            f"Thời điểm tạo: {fmt_time(job.created_at)}\\n"
            f"Thời điểm bấm Đăng: {fmt_time(job.sent_intent_at)}\\n"
            f"Liên kết bài viết tìm thấy: {job.permalink or 'Chưa có'}\\n"
            f"Thư mục bằng chứng/ảnh chụp: {job.evidence_path or 'Chưa có'}\\n"
            f"\\n{'=' * 70}\\nGHI CHÚ / LÝ DO ĐẦY ĐỦ\\n{'=' * 70}\\n"
            f"{job.note or 'Không có ghi chú.'}\\n"
            f"\\n{'=' * 70}\\nNỘI DUNG BÀI ĐĂNG ĐẦY ĐỦ\\n{'=' * 70}\\n"
            f"{job.content or '(Trống)'}\\n"
            f"\\n{'=' * 70}\\nDANH SÁCH ẢNH\\n{'=' * 70}\\n"
            f"{images_text}\\n"
        )
        body.insert("1.0", detail_text)
        body.configure(state="disabled")

        buttons = tk.Frame(detail, padx=14, pady=10)
        buttons.pack(fill="x")
        def copy_details():
            detail.clipboard_clear()
            detail.clipboard_append(detail_text)
            detail.update()
            messagebox.showinfo("Đã sao chép", "Đã sao chép toàn bộ chi tiết lịch sử.", parent=detail)

        tk.Button(buttons, text="Sao chép toàn bộ chi tiết", command=copy_details).pack(side="left")
        if job.permalink:
            tk.Button(buttons, text="Mở bài viết", command=lambda: webbrowser.open(job.permalink)).pack(side="left", padx=8)
        tk.Button(buttons, text="Đóng", command=detail.destroy).pack(side="right")

    def open_selected_group(self):
        job = self._selected()
        if job:
            webbrowser.open(job.permalink or job.group_url)

    def open_evidence(self):
        job = self._selected()
        if job and job.evidence_path:
            path = Path(job.evidence_path)
            if path.exists():
                if sys.platform == 'win32':
                    os.startfile(str(path))
                elif sys.platform == 'darwin':
                    subprocess.Popen(['open', str(path)])
                else:
                    subprocess.Popen(['xdg-open', str(path)])
            else:
                messagebox.showinfo('Chưa có bằng chứng', 'Không tìm thấy thư mục ảnh chụp trên máy.')
        elif job:
            messagebox.showinfo('Chưa có ảnh', 'Bài này chưa chạy, nên chưa có ảnh chụp.')

    def confirm_job(self):
        job = self._selected()
        if not job or job.status not in (REVIEW, PENDING, OBSERVED, ACKNOWLEDGED):
            return
        # Người dùng chỉ nhấn nút này sau khi trực tiếp nhìn thấy bài Facebook.
        # Ghi nhận một chạm, không đặt trạng thái CONFIRMED chỉ vì đã nhấn Đăng.
        self.store.confirm(job.id)
        self.refresh_jobs()
        self._log(f'Đã đánh dấu bài {job.id}: bạn đã xem và thấy đủ chữ, ảnh trên Facebook.')

    def retry_failed(self):
        job = self._selected()
        if not job:
            return
        if job.status != FAILED or job.sent_intent_at is not None:
            messagebox.showwarning('Không thể tự thử lại',
                'Chỉ thử lại bài LỖI TRƯỚC KHI GỬI. Với bài có thể đã nhấn Đăng, hãy kiểm tra Facebook trước.')
            return
        if messagebox.askyesno('Thử lại', 'Bài chưa hề nhấn Đăng theo nhật ký. Đưa vào hàng chờ lại?'):
            if self.store.retry_before_submit(job.id):
                self.current_ids = [job.id]
                self.refresh_jobs()
                self._log('Đã đưa lại vào hàng chờ. Nhấn “Tiếp tục nhóm còn lại” để chạy.')

    def _busy(self):
        if self.worker_running:
            messagebox.showinfo('Đang chạy', 'Hãy chờ công việc hiện tại kết thúc.')
            return True
        return False

    def install_dependencies(self):
        """Install and verify dependencies using this app's exact Python executable."""
        if self._busy():
            return
        python_exe = sys.executable
        version = sys.version.split()[0]
        if not messagebox.askyesno('Cài/Sửa thư viện',
                'Ứng dụng sẽ cài Playwright, Chromium và Pillow (xem ảnh thu nhỏ).\n\n'
                f'Python đang dùng: {python_exe}\nPhiên bản: {version}\n\n'
                'Cần Internet. Nếu đây là Python thử nghiệm chưa được Playwright hỗ trợ, '
                'hãy cài Python 3.12 hoặc 3.13 ổn định rồi mở ứng dụng bằng phiên bản đó. Tiếp tục?'):
            return
        self.worker_running = True
        def run_cmd(cmd, label):
            self.emit(label + ': ' + ' '.join(cmd))
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, encoding='utf-8', errors='replace', timeout=900)
            output = (result.stdout or '').strip()
            if output:
                for line in output.splitlines()[-14:]:
                    self.emit(line[:350])
            if result.returncode:
                raise RuntimeError(f'{label} thất bại (mã {result.returncode}). Xem log phía trên.')
        def worker():
            try:
                # Make sure pip is available, then install packages in the exact interpreter
                # that runs this GUI. Do not launch the browser-install command until import works.
                try:
                    run_cmd([python_exe, '-m', 'pip', '--version'], 'Kiểm tra pip')
                except Exception:
                    run_cmd([python_exe, '-m', 'ensurepip', '--upgrade'], 'Khởi tạo pip')
                run_cmd([python_exe, '-m', 'pip', 'install', '--upgrade', 'pip', 'setuptools', 'wheel'], 'Cập nhật pip')
                run_cmd([python_exe, '-m', 'pip', 'install', '--upgrade', 'playwright', 'Pillow'], 'Cài Playwright và Pillow')
                # Verify import with this same interpreter before installing browser binaries.
                run_cmd([python_exe, '-c', 'import playwright; from PIL import Image, ImageTk; print("PLAYWRIGHT_AND_PILLOW_OK")'], 'Kiểm tra thư viện')
                run_cmd([python_exe, '-m', 'playwright', 'install', 'chromium'], 'Cài Chromium')
                run_cmd([python_exe, '-c', 'from playwright.sync_api import sync_playwright; p=sync_playwright().start(); print(p.chromium.executable_path); p.stop()'], 'Kiểm tra Chromium')
                self.emit('CÀI THÀNH CÔNG: Playwright, Chromium và Pillow đã sẵn sàng. Hãy đóng rồi mở lại ứng dụng, sau đó đăng nhập Facebook.')
                self.ui_events.put(('refresh_photos', None))
            except subprocess.TimeoutExpired:
                self.emit('CÀI ĐẶT QUÁ THỜI GIAN: kiểm tra Internet rồi thử lại.')
            except Exception as exc:
                self.emit(f'LỖI CÀI ĐẶT: {type(exc).__name__}: {exc}')
                if sys.version_info >= (3, 15):
                    self.emit('Bạn đang dùng Python 3.15 RC. Nên cài Python 3.13 ổn định và chạy file bằng Python 3.13; cài trong Python khác sẽ không sửa môi trường của ứng dụng hiện tại.')
            finally:
                self.ui_events.put(('done', 'Đã kết thúc kiểm tra/cài đặt.'))
        threading.Thread(target=worker, daemon=True).start()

    def login(self):
        if self._busy():
            return
        self.worker_running = True
        def worker():
            try:
                login_browser(self.emit)
            except ImportError:
                self.emit('THIẾU PLAYWRIGHT: Nhấn nút Cài/Sửa Playwright màu cam, sau đó thử lại.')
            except Exception as exc:
                self.emit(f'Không mở được Chrome: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}')
            finally:
                self.ui_events.put(('done', 'Đã kết thúc bước đăng nhập.'))
        threading.Thread(target=worker, daemon=True).start()

    def start_new(self):
        if self._busy():
            return
        self.save()
        content = self.content_input.get('1.0', 'end-1c')
        group_text = self.groups_input.get('1.0', 'end-1c')
        try:
            groups = clean_groups(group_text)
            images = validate_images(self.images)
            if not content.strip():
                raise ValueError('Bạn chưa nhập chữ cho bài viết.')
            # Mỗi lần bấm Đăng là một lượt có chủ đích riêng; cho phép đăng lại
            # cùng nội dung/nhiều nhóm, nhưng các lượt vẫn cách nhau tối thiểu 30 phút.
            repeat_key = self.repeat_key_next or f'manual-{uuid.uuid4().hex}'
            ids, dup = self.store.add_jobs(content, images, groups, repeat_key=repeat_key)
            self.repeat_key_next = None
        except ValueError as exc:
            messagebox.showerror('Kiểm tra dữ liệu', str(exc))
            return
        self.current_ids = ids
        self.refresh_jobs()
        if dup:
            messagebox.showinfo('Chặn bài trùng',
                f'Đã bỏ qua {len(dup)} nhóm vì đã có cùng nội dung và ảnh trong lịch sử.\n'
                'Nếu từng nhấn Đăng mà chưa rõ kết quả, hãy kiểm tra trước để tránh đăng trùng.')
        if not ids:
            self._log('Bài đã có trong lịch sử. Nếu trạng thái LỖI TRƯỚC KHI GỬI, chọn bài tại Lịch sử → Thử lại lỗi trước khi gửi → Tiếp tục nhóm còn lại.')
            return
        if not messagebox.askyesno('Bắt đầu tự động',
            f'Chuẩn bị đăng {len(ids)} nhóm, {len(images)} ảnh/nhóm. Không nghỉ giữa các nhóm trong cùng lượt.\n'
            'Nếu bấm Đăng ngay cho lượt kế tiếp, lượt mới sẽ bắt đầu khi đã đủ 30 phút kể từ lúc lượt trước hoàn tất.\n\n'
            'Chỉ đăng khi bạn được nhóm cho phép. Không vượt CAPTCHA/2FA.\n'
            + ('Sẽ TỰ ĐỘNG sang nhóm khác kể cả khi nhóm trước lỗi hoặc chưa rõ kết quả.\n'
               if self.autocontinue.get() else 'Sẽ dừng sau mỗi nhóm.\n')
            + 'Không thử lại tự động bài bị lỗi / chưa rõ kết quả.\n'
            + '\nBắt đầu ngay?'):
            self._log('Đã lưu công việc mới. Có thể nhấn Tiếp tục nhóm còn lại sau.')
            return
        self._run_ids(ids, manual_round=True)

    def resume_queue(self):
        if self._busy():
            return
        ids = [id_ for id_ in self.current_ids if self.store.get(id_).status == QUEUED]
        if not ids:
            # On restart, old queued jobs may contain different posts. Ask first.
            ids = [job.id for job in reversed(self.store.all()) if job.status == QUEUED][:MAX_GROUPS]
            if ids and not messagebox.askyesno('Tiếp tục công việc cũ',
                f'Có {len(ids)} công việc chưa chạy từ lịch sử, có thể thuộc nhiều bài khác nhau.\n'
                'Bạn muốn thử đăng các công việc này không?'):
                return
        if not ids:
            messagebox.showinfo('Không có bài chờ', 'Không còn bài chưa đăng trong hàng chờ.')
            return
        self._run_ids(ids)

    def _run_ids(self, ids: list[int], *, delay_override: int | None = None,
                  schedule_id: str | None = None, manual_round: bool = False):
        self.stop_event.clear()
        self.worker_running = True
        self.tabs.select(self.histtab)
        # Các lượt thủ công: không nghỉ giữa nhóm. Lịch hẹn giữ nguyên cơ chế nghỉ hiện có.
        delay = max(0, int(delay_override if delay_override is not None else self.delay_seconds.get()))
        keep_going = self.autocontinue.get()
        schedule_status = 'finished'
        schedule_error = ''
        round_completed = True

        def worker():
            nonlocal schedule_status, schedule_error, round_completed
            try:
                # Chỉ nút Đăng ngay áp dụng thời gian nghỉ giữa HAI LƯỢT hoàn chỉnh.
                # Mốc thời gian được lưu bền vững trong SQLite, kể cả khi mở lại ứng dụng.
                if manual_round:
                    previous_round = self.store.get_app_state('manual_round_completed_at')
                    if previous_round:
                        try:
                            previous_at = datetime.fromisoformat(previous_round)
                            elapsed = (datetime.now().astimezone() - previous_at.astimezone()).total_seconds()
                        except (TypeError, ValueError):
                            elapsed = 1800
                        remaining_wait = max(0, int(1800 - elapsed))
                        if remaining_wait:
                            self.emit(f'Lượt trước đã hoàn tất; chờ {remaining_wait} giây để đủ 30 phút rồi mới bắt đầu lượt mới.')
                            if wait_before_next(self.stop_event, remaining_wait, self.emit):
                                round_completed = False
                                self.emit('Đã hủy lượt mới trong thời gian chờ; chưa bắt đầu đăng nhóm nào.')
                                return
                # Launch the browser ONCE and reuse it for every group.
                with browser_batch_session() as page:
                    for pos, job_id in enumerate(ids):
                        if self.stop_event.is_set():
                            round_completed = False
                            self.emit('Đã dừng theo yêu cầu. Các nhóm còn lại chưa chạy.')
                            break
                        job = self.store.get(job_id)
                        if job.status != QUEUED:
                            continue
                        self.emit(f'Nhóm {pos + 1}/{len(ids)}: {job.group_url}')
                        try:
                            # quy tắc chống gửi sát nhau cho lịch/tiếp tục hàng chờ;
                            # riêng nút Đăng ngay chỉ nghỉ giữa các lượt, không nghỉ giữa nhóm.
                            # Lịch hẹn: đến giờ đã chọn thì bắt đầu đăng ngay,
                            # không áp dụng điều kiện chờ 30 phút giữa các lần gửi cùng nội dung.
                            # Hàng chờ tiếp tục thủ công vẫn giữ cơ chế chống đăng sát nhau.
                            if not manual_round and not schedule_id:
                                previous = self.store.latest_sent_intent_for_payload(job.content, job.images, job.id)
                                if previous:
                                    try:
                                        previous_at = datetime.fromisoformat(previous)
                                        elapsed = (datetime.now().astimezone() - previous_at.astimezone()).total_seconds()
                                    except (TypeError, ValueError):
                                        elapsed = 1800
                                    remaining_wait = max(0, int(1800 - elapsed))
                                    if remaining_wait:
                                        self.emit(f'Cùng nội dung đã có lần gửi gần đây; chờ đủ 30 phút ({remaining_wait} giây còn lại).')
                                        if wait_before_next(self.stop_event, remaining_wait, self.emit):
                                            self.emit('Đã dừng theo yêu cầu trước khi gửi bài tiếp theo.')
                                            round_completed = False
                                            break
                            outcome = run_one(job, self.store, self.emit, page=page)
                        except Exception as exc:
                            # Group-specific error: don't abort the whole queue.
                            latest = self.store.get(job_id)
                            detail = f'{type(exc).__name__}: {str(exc).splitlines()[0][:250]}'
                            if latest.sent_intent_at:
                                outcome = REVIEW
                                self.store.transition(job_id, REVIEW,
                                    'Lỗi sau thao tác Đăng: ' + detail + '. Không tự thử lại.')
                            else:
                                outcome = FAILED
                                self.store.transition(job_id, FAILED, 'Bỏ qua do lỗi: ' + detail)
                            self.emit(f'Đã lưu lỗi nhóm {pos + 1}, sẽ chuyển nhóm mới.')
                        self.ui_events.put(('refresh', None))
                        self.emit(f'Nhóm {pos + 1}/{len(ids)}: {describe_outcome(outcome)}')
                        if not can_advance_automatically(outcome, keep_going):
                            round_completed = False
                            self.emit('Tạm dừng theo tùy chọn người dùng; lượt chưa chạy hết các nhóm đã chọn.')
                            break
                        remaining = [k for k in ids[pos+1:] if self.store.get(k).status == QUEUED]
                        if not remaining:
                            continue
                        if outcome in (FAILED, REVIEW, SKIPPED):
                            # Essential: NO fixed wait for an error or unverified click.
                            self.emit('Nhóm lỗi/chưa rõ: BỎ QUA NGAY, mở nhóm kế tiếp không chờ.')
                            continue
                        if not manual_round:
                            self.emit(f'Bài có tín hiệu gửi; nghỉ {delay} giây trước nhóm mới.')
                            if wait_before_next(self.stop_event, delay, self.emit):
                                round_completed = False
                                self.emit('Đã dừng theo yêu cầu.')
                                break
            except ImportError as exc:
                round_completed = False
                schedule_status = 'error'
                schedule_error = 'Thiếu Playwright: ' + str(exc)[:180]
                self.emit('Thiếu Playwright. Nhấn Cài/Sửa Playwright trên ứng dụng. ' + str(exc))
            except Exception as exc:
                round_completed = False
                schedule_status = 'error'
                schedule_error = f'{type(exc).__name__}: {str(exc).splitlines()[0][:180]}'
                self.emit(f'Lỗi chung (trình duyệt/môi trường): {type(exc).__name__}: {exc}')
            finally:
                if manual_round and round_completed and not self.stop_event.is_set():
                    try:
                        self.store.set_app_state('manual_round_completed_at', now())
                        self.emit('Đã hoàn tất lượt đăng. Mốc 30 phút nghỉ giữa lượt đã được lưu.')
                    except Exception as exc:
                        self.emit(f'Cảnh báo: không lưu được mốc hoàn tất lượt: {type(exc).__name__}')
                if schedule_id:
                    if self.stop_event.is_set() and schedule_status == 'finished':
                        schedule_status = 'stopped'
                    self.store.update_schedule(schedule_id, schedule_status, error=schedule_error)
                    self.ui_events.put(('refresh_schedules', None))
                done_text = ('Đã kết thúc lượt Đăng ngay; bài lỗi/chưa rõ được lưu, không tự đăng lại.'
                             if manual_round else 'Đã kết thúc lượt; bài lỗi/chưa rõ được lưu, không tự đăng lại.')
                self.ui_events.put(('done', done_text))
        threading.Thread(target=worker, daemon=True).start()

    def stop_after_current(self):
        self.stop_event.set()
        self._log('Đã yêu cầu dừng sau nhóm hiện tại. Không ngắt giữa thao tác bấm Đăng.')

    def _close(self):
        self.save()
        if self.worker_running and not messagebox.askyesno('Ứng dụng đang xử lý',
                'Ứng dụng còn chạy. Đóng giữa lúc đăng có thể khiến trạng thái chưa rõ.\nĐóng ngay?'):
            return
        self.root.destroy()


def main():
    root = tk.Tk()
    PosterApp(root)
    root.mainloop()


if __name__ == '__main__':
    main()
