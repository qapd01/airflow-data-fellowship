"""
Lab 3 — Hospital Pipeline (BROKEN VERSION)
==========================================
โจทย์: รับ broken DAG ที่ทีมคนก่อนเขียทิ้งไว้
       มีบั๊ก 3 จุดซ่อนอยู่ ต้องหาและแก้ให้ pipeline
       พร้อมรันก่อน 08:00 ของทุกเช้า

ภารกิจ:
  1. Import DAG นี้และดูว่ามี parse error ไหมใน Airflow UI
  2. Bug 1: task dependency ผิด — ทำให้ send_report รันก่อน aggregate
            แก้ลำดับให้ถูก: fetch_lab_results >> aggregate_stats >> send_report
  3. Bug 2: ไม่มี retry — เพิ่ม retries=3, retry_delay=timedelta(minutes=5)
            ใน default_args
  4. Bug 3: มี hardcoded credentials ใน connection string
            ย้ายไปเก็บใน Airflow Connection แล้วใช้ conn_id แทน
  5. Trigger DAG manually และตรวจสอบว่าทุก task สีเขียวใน UI

(Bonus) เพิ่ม FileSensor รอไฟล์ /tmp/lab_results_{ds}.csv ก่อน fetch
        ถ้าไฟล์ไม่มาภายใน poke_interval=60, timeout=1800 → task fail และส่ง alert
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import sqlite3
import random

# ❌ BUG 2: ไม่มี retries และ retry_delay!
default_args = {
    "owner": "hospital-data-team",
    "email": ["data-alert@hospital.th"],
    "email_on_failure": True,
}

# ❌ BUG 3: hardcoded credentials — อันตรายมาก!
# ห้ามเก็บ password ในโค้ดที่ commit ขึ้น git
DB_HOST     = "prod-db.hospital.internal"
DB_USER     = "admin"
DB_PASSWORD = "P@ssw0rd123!"   # ← อันตราย!
DB_NAME     = "hospital_db"
CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"


def get_mock_db():
    """ใช้ SQLite แทน PostgreSQL สำหรับ lab"""
    conn = sqlite3.connect("/tmp/hospital.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_results (
            patient_id TEXT,
            test_type  TEXT,
            result     TEXT,
            value      REAL,
            unit       TEXT,
            test_date  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            stat_date    TEXT,
            test_type    TEXT,
            total_tests  INTEGER,
            abnormal_cnt INTEGER
        )
    """)
    conn.commit()
    return conn


def generate_lab_results(date: str) -> list:
    random.seed(date)
    test_types = ["CBC", "Blood Sugar", "Liver Function", "Kidney Function", "Thyroid"]
    results = []
    for i in range(20):
        value = round(random.uniform(50, 200), 1)
        results.append({
            "patient_id": f"HN-{random.randint(10000, 99999)}",
            "test_type":  random.choice(test_types),
            "result":     "Normal" if value < 150 else "Abnormal",
            "value":      value,
            "unit":       "mg/dL",
            "test_date":  date,
        })
    return results


@dag(
    dag_id="lab3_hospital_pipeline",
    default_args=default_args,
    schedule="0 7 * * *",   # รันทุกวัน 07:00 (พร้อมก่อน 08:00)
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["lab3", "debug"],
    doc_md=__doc__,
)
def lab3_hospital_pipeline():

    @task
    def fetch_lab_results(ds=None):
        """ดึงผล lab จากระบบโรงพยาบาล"""
        print(f"[fetch] กำลังดึงผล lab วันที่ {ds}")
        # จำลองการเชื่อมต่อ DB (ใน lab ใช้ mock data)
        print(f"[fetch] (จะเชื่อม: {CONN_STRING[:30]}...)")
        results = generate_lab_results(ds)
        print(f"[fetch] ได้ {len(results)} ผลตรวจ")
        return results

    @task
    def aggregate_stats(results: list, ds=None):
        """รวบรวมสถิติรายวัน"""
        conn = get_mock_db()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM daily_stats WHERE stat_date = ?", (ds,))
            # นับแต่ละ test_type
            from collections import Counter
            type_count = Counter(r["test_type"] for r in results)
            abnormal   = Counter(r["test_type"] for r in results if r["result"] == "Abnormal")
            for test_type, total in type_count.items():
                cur.execute(
                    "INSERT INTO daily_stats VALUES (?, ?, ?, ?)",
                    (ds, test_type, total, abnormal.get(test_type, 0))
                )
            conn.commit()
            print(f"[aggregate] บันทึกสถิติ {len(type_count)} test types วัน {ds} ✅")
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

    @task
    def send_report(ds=None):
        """ส่งรายงานสรุปประจำวัน"""
        conn = get_mock_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT test_type, total_tests, abnormal_cnt
            FROM daily_stats WHERE stat_date = ?
            ORDER BY abnormal_cnt DESC
        """, (ds,))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            raise ValueError(f"ไม่มีข้อมูล daily_stats วัน {ds} — aggregate ยังไม่เสร็จ?")

        print(f"\n{'='*45}")
        print(f"Hospital Lab Report — {ds}")
        print(f"{'Test Type':<20} {'Total':>8} {'Abnormal':>10}")
        print(f"{'-'*45}")
        for test_type, total, abnormal in rows:
            flag = " ⚠️" if abnormal > 3 else ""
            print(f"{test_type:<20} {total:>8} {abnormal:>10}{flag}")
        print(f"{'='*45}")
        print("[send_report] ส่งรายงานไปยัง email แล้ว (mock) ✅\n")

    results = fetch_lab_results()

    # ❌ BUG 1: dependency ผิด! send_report รันก่อน aggregate
    # แก้ให้ถูก: results >> aggregate_stats(results) >> send_report()
    stats = aggregate_stats(results)
    send_report()          # ← ไม่มี dependency กับ stats เลย!


lab3_hospital_pipeline()