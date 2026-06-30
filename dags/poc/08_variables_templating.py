"""
=== POC 08: Variables & Jinja Templating ===
📖 Slide Reference: หน้า 39-42 (Variables, Templating, Context, Metadata)

แนวคิดสำคัญ:

1. Airflow Variables คืออะไร?
   - Variables คือ key-value store ที่เก็บใน Airflow metadata DB
   - ใช้เก็บ config ที่ต้องการเปลี่ยนได้โดยไม่ต้องแก้ code
   - จัดการผ่าน Airflow UI > Admin > Variables
   - หรือผ่าน code: Variable.get() / Variable.set()

2. Jinja Templating คืออะไร?
   - Airflow ใช้ Jinja2 template engine ใน parameters บางตัว
   - เช่น bash_command ของ BashOperator
   - Template variables ที่ใช้บ่อย:
     {{ ds }}              → logical_date เป็น YYYY-MM-DD
     {{ ds_nodash }}       → logical_date เป็น YYYYMMDD
     {{ dag.dag_id }}      → ชื่อ DAG
     {{ task.task_id }}    → ชื่อ task
     {{ run_id }}          → run ID
     {{ var.value.key }}   → ดึง Variable ใน template
     {{ dag_run.conf }}    → config จาก manual trigger

3. dag_run.conf คืออะไร?
   - เมื่อ trigger DAG manually สามารถส่ง JSON config ได้
   - ใช้สำหรับ parameterize DAG run
   - เช่น {"target_date": "2026-06-15", "mode": "full"}

Scenario: pipeline สร้าง report file โดยใช้ template สร้างชื่อไฟล์ dynamic
"""

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.models import Variable
from airflow.operators.bash import BashOperator


# ============================================================
# DAG Definition
# ============================================================
@dag(
    dag_id="poc_variables_templating_report",
    schedule=None,  # Manual trigger เท่านั้น
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["poc", "variables", "templating"],
    doc_md="""
    ## POC: Variables & Jinja Templating
    - สร้างและอ่าน Airflow Variables
    - ใช้ Jinja template ใน BashOperator
    - อ่าน dag_run.conf จาก manual trigger
    """,
)
def poc_variables_templating_report():

    # ----------------------------------------------------------
    # Task 1: สร้าง Variables สำหรับ demo
    # ----------------------------------------------------------
    @task
    def setup_variables():
        """
        ใช้ Variable.set() เพื่อสร้าง Variables ใน Airflow metadata DB

        ⚠️ หมายเหตุ:
        - Variable.set() จะ overwrite ถ้ามี key ซ้ำ
        - ในงานจริง ควรสร้างผ่าน UI หรือ CLI ไม่ใช่ใน DAG code
        - ที่ทำใน DAG นี้เพราะเป็น demo เท่านั้น
        """
        # สร้าง variable แบบ string ธรรมดา
        Variable.set("poc_report_path", "/tmp/reports")
        print("✅ Set variable: poc_report_path = /tmp/reports")

        # สร้าง variable แบบ JSON string
        # serialize_json=False เพราะเราส่ง string เข้าไปเอง
        Variable.set(
            "poc_api_config",
            '{"base_url": "https://api.example.com", "timeout": 30}',
            serialize_json=False,
        )
        print('✅ Set variable: poc_api_config = {"base_url": ..., "timeout": 30}')
        print()
        print("💡 ดู Variables ได้ที่ Airflow UI > Admin > Variables")

    # ----------------------------------------------------------
    # Task 2: อ่าน Variables 2 วิธี
    # ----------------------------------------------------------
    @task
    def read_variables():
        """
        มี 2 วิธีหลักในการอ่าน Variables:

        1. Variable.get() — ใช้ใน @task (Python code)
        2. {{ var.value.key }} — ใช้ใน Jinja template (BashOperator, etc.)

        ⚠️ ข้อควรระวัง:
        - Variable.get() จะ query DB ทุกครั้งที่เรียก
        - ถ้าใช้บ่อยเกินไปอาจทำให้ DB load สูง
        - ใช้ default parameter เพื่อป้องกัน KeyError
        """
        # Method 1: Variable.get() — ใช้ใน Python code
        report_path = Variable.get("poc_report_path")
        print(f"📂 Report path: {report_path}")

        # อ่าน JSON variable แล้ว deserialize เป็น dict
        # deserialize_json=True จะ json.loads() ให้อัตโนมัติ
        api_config = Variable.get("poc_api_config", deserialize_json=True)
        print(f"🔧 API Config: {api_config}")
        print(f"   Base URL: {api_config['base_url']}")
        print(f"   Timeout: {api_config['timeout']}")

        # ใช้ default value เพื่อป้องกัน KeyError
        missing_var = Variable.get("non_existent_var", default_var="default_value")
        print(f"❓ Missing var (with default): {missing_var}")

        # Method 2: จาก context (สำหรับ @task)
        # ใน Jinja template ใช้ {{ var.value.poc_report_path }}
        # แต่ใน @task ต้องใช้ Variable.get() แทน
        context = get_current_context()
        print()
        print("💡 ใน Jinja template (เช่น BashOperator) ใช้:")
        print("   {{ var.value.poc_report_path }}")
        print("   {{ var.json.poc_api_config.base_url }}")

    # ----------------------------------------------------------
    # Task 3: BashOperator กับ Jinja templates
    # ----------------------------------------------------------
    # BashOperator มี bash_command ที่เป็น "templated field"
    # Airflow จะ render Jinja template ก่อนส่งให้ bash execute
    demo_bash_templating = BashOperator(
        task_id="demo_bash_templating",
        bash_command=(
            'echo "DAG: {{ dag.dag_id }}" && '
            'echo "Task: {{ task.task_id }}" && '
            'echo "Date: {{ ds }}" && '
            'echo "Run ID: {{ run_id }}" && '
            'echo "No-dash: {{ ds_nodash }}" && '
            'echo "Report file: report_{{ ds_nodash }}.csv"'
        ),
    )

    # ----------------------------------------------------------
    # Task 4: อ่าน dag_run.conf จาก manual trigger
    # ----------------------------------------------------------
    @task
    def demo_dagrun_conf():
        """
        dag_run.conf คือ JSON config ที่ส่งมาตอน trigger DAG manually

        วิธี trigger พร้อม config:
        1. Airflow UI > DAGs > Trigger DAG w/ config
           ใส่ JSON: {"target_date": "2026-06-15", "mode": "full"}
        2. CLI: airflow dags trigger poc_variables_templating_report \\
                --conf '{"target_date": "2026-06-15"}'
        3. API: POST /api/v1/dags/.../dagRuns with body {"conf": {...}}
        """
        context = get_current_context()

        # ดึง conf จาก dag_run — ถ้าไม่มี config จะเป็น {} (empty dict)
        conf = context["dag_run"].conf or {}
        target = conf.get("target_date", "not specified")

        print("=" * 60)
        print("📋 DAG RUN CONFIGURATION")
        print("=" * 60)
        print(f"  Manual trigger config: {conf}")
        print(f"  Target date from conf: {target}")
        print()
        print("💡 Tip: Trigger DAG with config JSON to test this!")
        print('   Example config:')
        print('   {"target_date": "2026-06-15", "mode": "full"}')
        print()
        print("   วิธีใช้ config ในงานจริง:")
        print("   - กำหนด date range ที่ต้องการ re-process")
        print("   - เลือก mode (full/incremental)")
        print("   - ส่ง parameter พิเศษ เช่น target table, file path")
        print("=" * 60)

    # ----------------------------------------------------------
    # Dependencies
    # ----------------------------------------------------------
    # setup_vars >> [read_vars, bash_template] >> dagrun_conf
    # ใช้ list [] เพื่อ run read_variables และ demo_bash_templating พร้อมกัน (parallel)
    setup_vars = setup_variables()
    read_vars = read_variables()
    dagrun_conf = demo_dagrun_conf()

    setup_vars >> [read_vars, demo_bash_templating] >> dagrun_conf


# สร้าง DAG instance — จำเป็นต้องเรียก function เพื่อให้ Airflow detect DAG
poc_variables_templating_report()
