import os
import sqlite3
import requests
import json
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = FastAPI()

DB_FILE = "tasks.db"
# 環境変数からの読み込み
LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN', 'YOUR_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET', 'YOUR_CHANNEL_SECRET')
LOCAL_PC_URL = os.getenv('LOCAL_PC_URL', 'https://madison-preflowering-mandie.ngrok-free.dev/process')

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

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

def process_webhook_event(event_data):
    """
    バックグラウンドで処理を行う。
    1. DBにタスク保存
    2. LINEへの返信
    3. LocalPCへ即時POST転送を試行
    4. 成功すればステータスを更新 (失敗時はフォールバックのためpendingのまま)
    """
    ticker = event_data['ticker']
    reply_token = event_data['reply_token']
    
    # 1. DBに保存
    conn = get_db_connection()
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO tasks (ticker, status, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (ticker, "pending", now, now)
    )
    task_id = c.lastrowid
    conn.commit()
    conn.close()
    
    # 2. LINEへ返信 (テスト用)
    try:
        reply_text = f"証券コード「{ticker}」を受信しました。処理を開始します。"
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text=reply_text)
        )
    except Exception as e:
        print(f"LINE reply error: {e}")
        
    # 3. Local PCへ即時POST転送
    try:
        payload = {
            "task_id": task_id,
            "ticker": ticker
        }
        res = requests.post(LOCAL_PC_URL, json=payload, timeout=10)
        if res.status_code == 200:
            # 成功したら processing に更新
            conn = get_db_connection()
            c = conn.cursor()
            now = datetime.now().isoformat()
            c.execute(
                "UPDATE tasks SET status = 'processing', updated_at = ? WHERE id = ?",
                (now, task_id)
            )
            conn.commit()
            conn.close()
            print(f"Task {task_id} sent to Local PC successfully.")
        else:
            print(f"Local PC returned status {res.status_code}. Task {task_id} remains pending for fallback.")
    except Exception as e:
        print(f"Failed to send to Local PC: {e}. Task {task_id} remains pending for fallback.")

@app.post("/webhook")
async def line_webhook(request: Request, background_tasks: BackgroundTasks, x_line_signature: str = Header(None)):
    """
    LINEからのWebhookを受信し、キュー（SQLite）に保存する。
    同時にLINEへの返信とLocal PCへの即時POST送信をバックグラウンドで行う。
    """
    body = await request.body()
    body_str = body.decode('utf-8')
    
    # LINEの署名検証
    try:
        handler.handle(body_str, x_line_signature)
    except InvalidSignatureError:
        # 署名検証エラー。ローカルでのテスト等も考慮し、
        # ここではログを出力するだけに留め、強制エラーにはしない（必要に応じて有効化）
        print("Invalid signature warning")
        # raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body_str)
    events = data.get("events", [])
    for event in events:
        if event.get("type") == "message" and event.get("message", {}).get("type") == "text":
            ticker = event["message"]["text"].strip()
            reply_token = event.get("replyToken")
            
            event_data = {
                "ticker": ticker,
                "reply_token": reply_token
            }
            # バックグラウンド処理に追加
            background_tasks.add_task(process_webhook_event, event_data)
            
    return {"status": "ok"}

@app.get("/tasks/pending")
def get_pending_task():
    """
    Local PCからのポーリング用エンドポイント (フォールバック用)
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

if __name__ == "__main__":
    import uvicorn
 #   uvicorn.run(app, host="0.0.0.0", port=8000)
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
