from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Medical Data Warehouse API")

# Allow your dashboard to talk to your API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="medical_warehouse",
        user="postgres",
        password="bezawit1616@" 
    )

@app.get("/")
def home():
    return {"status": "API is active", "project": "Medical Telegram Warehouse"}

@app.get("/messages")
def get_messages(limit: int = 20):
    """Fetch the latest transformed messages from the Fact table"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM staging.fct_messages LIMIT %s", (limit,))
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

@app.get("/channels")
def get_channels():
    """Fetch all unique medical channels from the Dimension table"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM staging.dim_channels")
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data