"""
Lab 2 — YouTube Trending Pipeline (STARTER)
============================================
โจทย์: สร้าง pipeline ดึง mock data วิดีโอ trending ของไทย
       บันทึกลง SQLite วันละครั้ง
       ต้อง backfill ย้อนหลัง 7 วันได้โดยไม่มีข้อมูลซ้ำ

ภารกิจ:
  1. เติมโค้ดใน TODO ทุกจุด
  2. รัน DAG แล้วดูผลใน Airflow UI
  3. ทดสอบ backfill:
       airflow dags backfill lab2_youtube_trending -s 2026-06-01 -e 2026-06-07
  4. Query DB ตรวจสอบว่าแต่ละวันมีข้อมูลครบและไม่ซ้ำ:
       SELECT trend_date, COUNT(*) as cnt FROM trending GROUP BY trend_date ORDER BY trend_date;
  5. (Bonus) เพิ่ม task ที่ 4: พิมพ์ Top 3 วิดีโอของวันนั้น

ใช้ macro: {{ ds }} = วันที่ในรูปแบบ YYYY-MM-DD
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import sqlite3
import random

DB_PATH = "/tmp/youtube_trending.db"

default_args = {
    "owner": "data-team",
    # TODO 1: เพิ่ม retries=2 และ retry_delay=timedelta(minutes=2)
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trending (
            video_id   TEXT,
            title      TEXT,
            channel    TEXT,
            views      INTEGER,
            likes      INTEGER,
            trend_date TEXT
        )
    """)
    conn.commit()
    return conn


def mock_trending_api(date: str) -> list:
    """Mock YouTube Data API — seed เดิม = ผลลัพธ์เดิม (idempotent โดยธรรมชาติ)"""
    random.seed(date)
    channels = ["WorkpointOfficial", "OneHD", "GMM Grammy", "Gmmtv", "Modernine TV"]
    titles = [
        "เพลงใหม่มาแรง", "ซีรีย์ไทยฮิต", "ข่าวเช้า", "รายการวาไรตี้",
        "คอนเสิร์ตสด", "ฮาวทู", "รีวิวอาหาร", "วล็อกท่องเที่ยว",
        "เกมมิ่ง", "แข่งกีฬา"
    ]
    videos = []
    for i in range(10):
        videos.append({
            "video_id":  f"VDO-{date}-{i+1:03d}",
            "title":     f"{random.choice(titles)} EP.{random.randint(1,100)}",
            "channel":   random.choice(channels),
            "views":     random.randint(100_000, 5_000_000),
            "likes":     random.randint(1_000, 200_000),
            "trend_date": date,
        })
    return videos


@dag(
    dag_id="lab2_youtube_trending",
    default_args=default_args,
    # TODO 2: กำหนด schedule="@daily"
    # TODO 3: กำหนด start_date=days_ago(7)
    # TODO 4: เปิด catchup=True เพื่อให้ backfill ได้
    tags=["lab2", "backfill", "templating"],
    doc_md=__doc__,
)
def lab2_youtube_trending():

    @task
    def fetch(ds=None):
        """
        TODO 5: เรียก mock_trending_api(ds) แล้ว return ผลลัพธ์
        ds คือวันที่ในรูปแบบ "YYYY-MM-DD" ที่ Airflow inject ให้อัตโนมัติ
        """
        print(f"[fetch] กำลังดึง trending วันที่ {ds}")
        # TODO 5: เติมโค้ดที่นี่
        pass

    @task
    def clean(videos: list) -> list:
        """
        TODO 6: กรองวิดีโอที่มี views < 500,000 ออก
        แล้ว return เฉพาะวิดีโอที่ views >= 500,000
        """
        # TODO 6: เติมโค้ดที่นี่
        pass

    @task
    def load(videos: list, ds=None):
        """
        TODO 7: บันทึกลง DB แบบ idempotent
        คำใบ้: ใช้ DELETE trend_date = ds ก่อน แล้วค่อย INSERT
        อย่าลืมใช้ transaction (try/except/rollback)
        """
        conn = get_db()
        cur = conn.cursor()
        try:
            # TODO 7: เติมโค้ดที่นี่
            pass
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

    # TODO 8: เชื่อม tasks ด้วย >> ให้ถูกลำดับ: fetch → clean → load
    raw = fetch()
    # TODO 8: เติมโค้ดที่นี่


lab2_youtube_trending()