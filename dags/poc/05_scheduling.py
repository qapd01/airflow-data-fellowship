"""
05_scheduling.py — 3 วิธี Scheduling ใน Airflow
=================================================
📖 Slide Reference: หน้า 34-36 (Scheduling: Cron, Timedelta, Execution Logic)

ไฟล์นี้สาธิต 3 วิธีในการกำหนด schedule ให้ DAG:
1. Cron Expression     — schedule="30 7 * * *" (รันทุกวันตอน 07:30)
2. Timedelta Interval  — schedule=timedelta(hours=6) (รันทุก 6 ชั่วโมง)
3. Timetable           — schedule=CronTriggerTimetable(...) (ยืดหยุ่นที่สุด)

Scenario: Temperature Sensor Data Collection
เก็บข้อมูลอุณหภูมิจาก sensor ด้วย schedule ที่ต่างกัน

============================================================================
📅 CRON EXPRESSION CHEAT SHEET
============================================================================

โครงสร้าง:  minute  hour  day_of_month  month  day_of_week
             (0-59) (0-23)   (1-31)     (1-12)   (0-6, 0=Sun)

ตัวอย่างที่ใช้บ่อย:
┌─────────────────────┬──────────────────────┬─────────────────────────────┐
│ Cron Expression     │ ความถี่               │ คำอธิบาย                     │
├─────────────────────┼──────────────────────┼─────────────────────────────┤
│ * * * * *           │ ทุกนาที              │ ทุก 1 นาที                   │
│ */5 * * * *         │ ทุก 5 นาที           │ 00:00, 00:05, 00:10, ...    │
│ */15 * * * *        │ ทุก 15 นาที          │ 00:00, 00:15, 00:30, ...    │
│ 0 * * * *           │ ทุกชั่วโมง            │ ต้นชั่วโมง (xx:00)           │
│ 0 0 * * *           │ ทุกวัน (เที่ยงคืน)     │ 00:00 ทุกวัน                 │
│ 30 7 * * *          │ ทุกวัน 07:30          │ 07:30 ทุกวัน                 │
│ 0 0 * * 0           │ ทุกอาทิตย์            │ เที่ยงคืนวันอาทิตย์           │
│ 0 0 1 * *           │ ทุกเดือน             │ วันที่ 1 ของทุกเดือน          │
│ 0 0 1 1 *           │ ทุกปี               │ 1 ม.ค. ทุกปี                 │
│ 0 9-17 * * 1-5      │ วันจันทร์-ศุกร์ 9-17  │ ทุกชั่วโมง ช่วงทำงาน          │
│ 0 0 * * 1-5         │ วันทำงาน             │ เที่ยงคืน จันทร์-ศุกร์          │
└─────────────────────┴──────────────────────┴─────────────────────────────┘

Special characters:
  *   = ทุกค่า (every)
  */N = ทุก N หน่วย (every N)
  ,   = หลายค่า (e.g., 1,15 = วันที่ 1 และ 15)
  -   = ช่วง (e.g., 1-5 = จันทร์ถึงศุกร์)

Airflow Preset Strings (สะดวกแต่จำกัด):
  @once    = รันครั้งเดียว
  @hourly  = 0 * * * *
  @daily   = 0 0 * * *
  @weekly  = 0 0 * * 0
  @monthly = 0 0 1 * *
  @yearly  = 0 0 1 1 *
============================================================================
"""

from datetime import timedelta

from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum


# ============================================================================
# DAG 1: Cron Expression — schedule="30 7 * * *"
# ============================================================================
# Cron expression เป็นวิธีที่ใช้กันมากที่สุดในการกำหนด schedule
# เหมาะเมื่อต้องการรันตาม "เวลาที่แน่นอน" เช่น ทุกวัน 07:30 น.
#
# ⚠️ สิ่งที่ต้องระวัง:
#   - Cron ใน Airflow ใช้ UTC เป็น default (ถ้าไม่กำหนด timezone)
#   - start_date กำหนดจุดเริ่มต้น — DAG จะไม่รันก่อนวันนี้
#   - catchup=False หมายถึงไม่ต้อง backfill run ที่พลาดไป

@dag(
    dag_id="poc_schedule_cron",
    # Cron expression: นาที=30, ชั่วโมง=7 → รันทุกวัน 07:30
    schedule="30 7 * * *",
    start_date=pendulum.datetime(2026, 1, 1),
    # catchup=False: ไม่ต้อง backfill run ที่พลาดไป
    # ถ้า True: Airflow จะรัน DAG ย้อนหลังทุก interval ตั้งแต่ start_date ถึงปัจจุบัน
    # ถ้า False: รันเฉพาะ interval ล่าสุดเท่านั้น
    catchup=False,
    tags=["poc", "scheduling"],
    doc_md="""
    ### Cron Schedule
    รันทุกวัน 07:30 UTC ด้วย cron expression `30 7 * * *`
    """,
)
def cron_schedule_pipeline():
    """
    Pipeline เก็บข้อมูลอุณหภูมิด้วย Cron Schedule
    รันทุกวัน 07:30 UTC
    """

    @task
    def collect_temperature_cron():
        """
        เก็บข้อมูลอุณหภูมิจาก sensor (Cron Schedule)
        """
        import random

        temp = round(random.uniform(20.0, 40.0), 1)
        print("🌡️ [Cron Schedule] Temperature Sensor Reading")
        print(f"   Schedule: 30 7 * * * (ทุกวัน 07:30 UTC)")
        print(f"   Temperature: {temp}°C")
        print(f"   Timestamp: {pendulum.now('Asia/Bangkok').to_datetime_string()}")

        # 💡 Cron Expression Breakdown:
        # 30  = นาทีที่ 30
        # 7   = ชั่วโมงที่ 7 (07:00)
        # *   = ทุกวัน
        # *   = ทุกเดือน
        # *   = ทุกวันในสัปดาห์
        # → รวม: รันทุกวัน เวลา 07:30

        return {"type": "cron", "temperature": temp}

    collect_temperature_cron()


# ⚠️ instantiate DAG
cron_schedule_pipeline()


# ============================================================================
# DAG 2: Timedelta Interval — schedule=timedelta(hours=6)
# ============================================================================
# Timedelta ใช้เมื่อต้องการรันทุก ๆ ช่วงเวลาที่กำหนด
# ต่างจาก Cron ตรงที่:
#   - Cron: รันตาม "เวลานาฬิกา" (e.g., 07:30 ทุกวัน)
#   - Timedelta: รันทุก ๆ "ระยะเวลา" (e.g., ทุก 6 ชั่วโมง จาก start_date)
#
# 💡 ใช้เมื่อไหร่:
#   - ต้องการรันทุก X นาที/ชั่วโมง โดยไม่สนใจเวลานาฬิกา
#   - Batch processing ที่ต้องการ interval คงที่
#   - เช่น: ทุก 30 นาที, ทุก 2 ชั่วโมง, ทุก 12 ชั่วโมง

@dag(
    dag_id="poc_schedule_timedelta",
    # timedelta(hours=6): รันทุก 6 ชั่วโมง จาก start_date
    # ไม่สนใจว่าจะตรงเวลาอะไร — แค่ห่างกัน 6 ชั่วโมง
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "scheduling"],
    doc_md="""
    ### Timedelta Schedule
    รันทุก 6 ชั่วโมง ด้วย `timedelta(hours=6)`
    """,
)
def timedelta_schedule_pipeline():
    """
    Pipeline เก็บข้อมูลอุณหภูมิด้วย Timedelta Schedule
    รันทุก 6 ชั่วโมง
    """

    @task
    def collect_temperature_timedelta():
        """
        เก็บข้อมูลอุณหภูมิจาก sensor (Timedelta Schedule)
        """
        import random

        temp = round(random.uniform(20.0, 40.0), 1)
        print("🌡️ [Timedelta Schedule] Temperature Sensor Reading")
        print(f"   Schedule: timedelta(hours=6) (ทุก 6 ชั่วโมง)")
        print(f"   Temperature: {temp}°C")
        print(f"   Timestamp: {pendulum.now('Asia/Bangkok').to_datetime_string()}")

        # 💡 Timedelta vs Cron:
        # timedelta(hours=6) → รัน 00:00, 06:00, 12:00, 18:00, ... (จาก start_date)
        # cron "0 */6 * * *"  → รัน 00:00, 06:00, 12:00, 18:00 (ตามนาฬิกา)
        # ดูเหมือนกัน แต่ timedelta จะ "เลื่อน" ถ้า start_date ไม่ตรงต้นชั่วโมง

        return {"type": "timedelta", "temperature": temp}

    collect_temperature_timedelta()


# ⚠️ instantiate DAG
timedelta_schedule_pipeline()


# ============================================================================
# DAG 3: Timetable — CronTriggerTimetable (ยืดหยุ่นที่สุด)
# ============================================================================
# Timetable เป็นวิธีที่ยืดหยุ่นที่สุดในการกำหนด schedule
# สามารถกำหนด timezone, business hours, custom logic ได้
#
# CronTriggerTimetable vs Cron string:
#   - Cron string: ใช้ UTC เป็น default, กำหนด timezone ไม่ได้โดยตรง
#   - CronTriggerTimetable: กำหนด timezone ได้ เช่น "Asia/Bangkok"
#     → รันตามเวลาท้องถิ่น ไม่ต้องแปลง UTC เอง!
#
# 💡 ใช้เมื่อไหร่:
#   - ต้องการ timezone-aware scheduling
#   - ต้องการ custom scheduling logic ที่ cron ทำไม่ได้
#   - ต้องการ data interval ที่ชัดเจน

@dag(
    dag_id="poc_schedule_timetable",
    # CronTriggerTimetable: รัน 08:00 เวลาไทย (Asia/Bangkok)
    # ต่างจาก cron "0 8 * * *" ที่จะรัน 08:00 UTC (= 15:00 ไทย)
    schedule=CronTriggerTimetable(
        "0 8 * * *",
        timezone="Asia/Bangkok",
    ),
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "scheduling"],
    doc_md="""
    ### Timetable Schedule
    รันทุกวัน 08:00 เวลาไทย ด้วย `CronTriggerTimetable`
    พร้อม timezone `Asia/Bangkok`
    """,
)
def timetable_schedule_pipeline():
    """
    Pipeline เก็บข้อมูลอุณหภูมิด้วย Timetable
    รันทุกวัน 08:00 เวลาไทย (Asia/Bangkok)
    """

    @task
    def collect_temperature_timetable():
        """
        เก็บข้อมูลอุณหภูมิจาก sensor (Timetable Schedule)
        """
        import random

        temp = round(random.uniform(20.0, 40.0), 1)
        print("🌡️ [Timetable Schedule] Temperature Sensor Reading")
        print(f"   Schedule: CronTriggerTimetable('0 8 * * *', timezone='Asia/Bangkok')")
        print(f"   Temperature: {temp}°C")
        print(f"   Timestamp: {pendulum.now('Asia/Bangkok').to_datetime_string()}")

        # 💡 CronTriggerTimetable vs Cron string:
        #
        # | วิธี                     | schedule                              | เวลาที่รัน (ไทย)  |
        # |-------------------------|---------------------------------------|-----------------|
        # | Cron string             | "0 8 * * *"                           | 15:00 (UTC+7)   |
        # | CronTriggerTimetable    | CronTriggerTimetable("0 8 * * *",     | 08:00 (UTC+7)   |
        # |                         |   timezone="Asia/Bangkok")            |                 |
        #
        # ถ้าต้องการรันตามเวลาไทย → ใช้ CronTriggerTimetable
        # ถ้าใช้ cron string ตรง ๆ ต้องแปลง UTC เอง (08:00 ไทย = 01:00 UTC → "0 1 * * *")

        return {"type": "timetable", "temperature": temp}

    collect_temperature_timetable()


# ⚠️ instantiate DAG
timetable_schedule_pipeline()


# ============================================================================
# 📝 สรุปเปรียบเทียบ 3 วิธี Scheduling:
# ============================================================================
#
# | วิธี                   | ตัวอย่าง                                    | ข้อดี                    | ข้อเสีย                  |
# |-----------------------|---------------------------------------------|------------------------|------------------------|
# | Cron Expression       | "30 7 * * *"                                | เข้าใจง่าย, ใช้กันทั่วไป  | ไม่มี timezone support   |
# | Timedelta             | timedelta(hours=6)                          | interval คงที่           | ไม่ผูกกับเวลานาฬิกา      |
# | CronTriggerTimetable  | CronTriggerTimetable("0 8 * * *", tz=...)   | timezone-aware          | ต้อง import เพิ่ม        |
#
# 💡 แนะนำ:
#   - งาน batch ที่ต้องรันตามเวลา → ใช้ Cron หรือ Timetable
#   - งานที่ต้องรันทุก X ชั่วโมง → ใช้ Timedelta
#   - งานที่ต้อง timezone ชัดเจน → ใช้ CronTriggerTimetable
