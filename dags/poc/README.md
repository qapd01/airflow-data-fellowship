# 📚 POC DAGs — ตัวอย่าง DAG สำหรับแต่ละหัวข้อจาก Slide

ไฟล์ตัวอย่างในโฟลเดอร์นี้ออกแบบมาเพื่อให้ผู้เรียนได้ลองรัน/ศึกษาแต่ละ concept จาก slide จริงๆ
ทุกไฟล์ self-contained — รันได้เลยใน Airflow UI โดยไม่ต้องพึ่ง external system (ยกเว้น `07` ที่ใช้ connection ที่มีอยู่แล้ว)

> 📖 Slide: `docs/Gray and White Simple Thesis Defense Presentation.pdf`

---

## 📑 สารบัญ

| # | ไฟล์ | 📖 Slide | DAG ID(s) | โจทย์จำลอง |
|---|------|----------|-----------|-----------|
| 01 | [01_dag_declaration.py](01_dag_declaration.py) | หน้า 30 | `poc_declaration_context_manager`<br>`poc_declaration_constructor`<br>`poc_declaration_decorator` | 🛒 สร้างรายงานยอดขาย — สาธิต 3 วิธีประกาศ DAG |
| 02 | [02_operators.py](02_operators.py) | หน้า 31 | `poc_operators_health_check` | 🏥 Health Check — BashOperator, PythonOperator, @task |
| 03 | [03_task_dependencies.py](03_task_dependencies.py) | หน้า 32 | `poc_dependencies_ecommerce` | 📦 E-Commerce Orders — >>, <<, fan-out/fan-in |
| 04 | [04_default_args.py](04_default_args.py) | หน้า 33 | `poc_default_args_warehouse` | 🏭 Data Warehouse ETL — default_args + task override |
| 05 | [05_scheduling.py](05_scheduling.py) | หน้า 34-36 | `poc_schedule_cron`<br>`poc_schedule_timedelta`<br>`poc_schedule_timetable` | 🌡️ Sensor Data — Cron, Timedelta, Timetable |
| 06 | [06_data_interval.py](06_data_interval.py) | หน้า 37, 40-41 | `poc_data_interval_logs` | 📋 Log Processing — Data Interval & Logical Date |
| 07 | [07_connections_hooks.py](07_connections_hooks.py) | หน้า 38 | `poc_connections_hooks_demo` | 🔌 Connection Demo — BaseHook, MinIO/ClickHouse |
| 08 | [08_variables_templating.py](08_variables_templating.py) | หน้า 39-42 | `poc_variables_templating_report` | 📝 Dynamic Report — Variables, Jinja, dag_run.conf |
| 09 | [09_catchup_backfill.py](09_catchup_backfill.py) | หน้า 10-16, 24 | `poc_catchup_backfill_demo` | ⏪ Backfill 3 วัน — catchup=True + Idempotency |
| 10 | [10_callbacks.py](10_callbacks.py) | หน้า 29 | `poc_callbacks_alerting` | 🔔 Alert System — on_success/failure/retry callbacks |

---

## 🚀 วิธีใช้งาน

### 1. Start Airflow

```bash
docker compose up -d
```

### 2. เปิด Airflow UI

เข้า [http://localhost:8080](http://localhost:8080) (login: `admin` / `admin`)

### 3. ค้นหา DAG

- ค้นหา tag **`poc`** ใน filter
- หรือพิมพ์ `poc_` ในช่อง search

### 4. Trigger DAG

- คลิกปุ่ม ▶️ (Trigger DAG) ที่ DAG ที่ต้องการ
- ดู log ของแต่ละ task เพื่อเห็นผลลัพธ์

---

## 💡 Tips

### ทดสอบ `dag_run.conf` (ไฟล์ 08)

กด **Trigger DAG w/ config** แล้วใส่ JSON:

```json
{"target_date": "2026-07-01", "report_type": "summary"}
```

### ดู Catchup & Backfill ทำงาน (ไฟล์ 09)

เปิด DAG `poc_catchup_backfill_demo` แล้วจะเห็น Airflow สร้าง DAG Run ย้อนหลัง 3 วันอัตโนมัติ (เพราะ `catchup=True`)

### ดู Callbacks ทำงาน (ไฟล์ 10)

DAG `poc_callbacks_alerting` มี task ที่ล้มเหลวแบบ random — ลองรันหลายรอบเพื่อเห็นทั้ง success และ failure callbacks

---

## 📁 โครงสร้างไฟล์

```
dags/poc/
├── 01_dag_declaration.py      # Slide 30 — 3 วิธีประกาศ DAG
├── 02_operators.py            # Slide 31 — Bash / Python / @task
├── 03_task_dependencies.py    # Slide 32 — >>, <<, fan-out/fan-in
├── 04_default_args.py         # Slide 33 — default_args + override
├── 05_scheduling.py           # Slide 34-36 — Cron / Timedelta / Timetable
├── 06_data_interval.py        # Slide 37, 40-41 — Data Interval & Logical Date
├── 07_connections_hooks.py    # Slide 38 — Connections & Hooks
├── 08_variables_templating.py # Slide 39-42 — Variables & Jinja Templating
├── 09_catchup_backfill.py     # Slide 10-16, 24 — Catchup & Backfill
├── 10_callbacks.py            # Slide 29 — Callbacks (Event Handlers)
└── README.md                  # ← คุณอยู่ที่นี่
```
