import os
import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

DB_FILE = "tasks.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# 初期化
init_db()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/")
def read_root():
    return {"message": "Welcome to Stock API (Webhook Receiver)"}

@app.post("/webhook")
async def line_webhook(request: Request):
    """
    LINEからのWebhookを受信し、キュー（SQLite）に保存する
    ※本来は署名検証等が必要ですが、ここではメッセージテキストをtickerとして抽出する簡易実装
    """
    body = await request.json()
    
    # LINE webhookイベントからテキストメッセージを抽出（簡易的）
    events = body.get("events", [])
    for event in events:
        if event.get("type") == "message" and event.get("message", {}).get("type") == "text":
            ticker = event["message"]["text"].strip()
            
            # DBに保存
            conn = get_db_connection()
            c = conn.cursor()
            now = datetime.now().isoformat()
            c.execute(
                "INSERT INTO tasks (ticker, status, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (ticker, "pending", now, now)
            )
            conn.commit()
            conn.close()
            
    return {"status": "ok"}

@app.get("/tasks/pending")
def get_pending_task():
    """
    Local PCからのポーリング用エンドポイント
    pending状態のタスクを1件取得し、processingに状態を更新して返す
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    # トランザクションとして処理
    c.execute("BEGIN TRANSACTION")
    
    # pendingなタスクを1件取得
    c.execute("SELECT * FROM tasks WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
    task = c.fetchone()
    
    if task:
        task_id = task['id']
        now = datetime.now().isoformat()
        # processingに更新
        c.execute(
            "UPDATE tasks SET status = 'processing', updated_at = ? WHERE id = ?",
            (now, task_id)
        )
        conn.commit()
        conn.close()
        return dict(task)
    else:
        conn.commit()
        conn.close()
        return {"message": "No pending tasks"}

class TaskResult(BaseModel):
    status: str
    result: Optional[str] = None

@app.post("/tasks/{task_id}/result")
def update_task_result(task_id: int, result_data: TaskResult):
    """
    Local PCから処理結果を受け取り、DBを更新する
    """
    conn = get_db_connection()
    c = conn.cursor()
    now = datetime.now().isoformat()
    
    c.execute(
        "UPDATE tasks SET status = ?, result = ?, updated_at = ? WHERE id = ?",
        (result_data.status, result_data.result, now, task_id)
    )
    conn.commit()
    conn.close()
    
    return {"status": "success"}
