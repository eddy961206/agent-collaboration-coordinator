"""Durable command receipt. Ambiguous crashes are held, never blindly replayed."""
import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


class Journal:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, hash TEXT NOT NULL, task TEXT, state TEXT NOT NULL, result TEXT, updated REAL)')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(commands)')}
            if 'payload' not in columns:
                db.execute('ALTER TABLE commands ADD COLUMN payload TEXT')
            db.execute("UPDATE commands SET state='unknown' WHERE state='processing'")
            db.execute('CREATE TABLE IF NOT EXISTS status_comments (task TEXT PRIMARY KEY, comment_id INTEGER NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS status_posts (task TEXT PRIMARY KEY, state TEXT NOT NULL)')
            db.execute("UPDATE status_posts SET state='unknown' WHERE state='sending'")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def begin(self, key, task, payload):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM commands WHERE id=?', (key,)).fetchone()
            if row:
                if row['task'] != task or row['hash'] != digest(payload):
                    raise ValueError('command_id_payload_conflict')
                return {'new': False, 'state': row['state'], 'result': json.loads(row['result']) if row['result'] else None}
            db.execute('INSERT INTO commands(id,hash,task,state,result,updated,payload) VALUES(?,?,?,?,?,?,?)',
                       (key, digest(payload), task, 'processing', None, time.time(), json.dumps(payload)))
            return {'new': True, 'state': 'processing'}

    def finish(self, key, state, result):
        with self.connect() as db:
            db.execute('UPDATE commands SET state=?,result=?,updated=? WHERE id=?', (state, json.dumps(result), time.time(), key))

    def status_comment(self, task, comment_id=None):
        with self.connect() as db:
            if comment_id is not None:
                db.execute('INSERT INTO status_comments VALUES(?,?) ON CONFLICT(task) DO UPDATE SET comment_id=excluded.comment_id', (task, comment_id))
            row = db.execute('SELECT comment_id FROM status_comments WHERE task=?', (task,)).fetchone()
            return row[0] if row else None

    def unresolved(self):
        with self.connect() as db:
            result = [dict(x) for x in db.execute("SELECT id,task,state,payload FROM commands WHERE state IN ('processing','unknown')")]
            for row in result:
                row['payload'] = json.loads(row['payload']) if row['payload'] else None
            return result

    def reserve_status_post(self, task):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT state FROM status_posts WHERE task=?', (task,)).fetchone()
            if row:
                return False
            db.execute("INSERT INTO status_posts VALUES(?,'sending')", (task,))
            return True

    def finish_status_post(self, task, known):
        with self.connect() as db:
            db.execute('UPDATE status_posts SET state=? WHERE task=?', ('known' if known else 'unknown', task))
