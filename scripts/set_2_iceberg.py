import os
import sys

# 1. ตั้งค่า Path พื้นฐาน
java_home = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
hadoop_home = r"C:\hadoop"

os.environ["JAVA_HOME"] = java_home
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.path.join(java_home, "bin") + ";" + os.path.join(hadoop_home, "bin") + ";" + os.environ["PATH"]

from pyspark.sql import SparkSession

# 2. สร้าง Spark Session พร้อม Config สำหรับ Apache Iceberg 
# ใช้ 'local' catalog และเก็บข้อมูลไว้ที่โฟลเดอร์ warehouse
spark = SparkSession.builder \
    .appName("Healthcare_Iceberg_Job") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "warehouse") \
    .getOrCreate()

print("--- Step 1: Loading Raw Data ---")
input_path = "data/healthcare_dataset.csv"
df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

# Step 2: Create & Insert Data into Iceberg Table (Init) 
print("--- Step 2: Initializing Iceberg Table ---")
# สร้างตารางชื่อ healthcare_table ภายใต้ catalog 'local' และ db 'db'
df_raw.writeTo("local.db.healthcare_table").createOrReplace()

# Step 3: Delete Record using SQL 
print("--- Step 3: Deleting Record (Bobby Jackson) using Iceberg SQL ---")
# ตรวจสอบจำนวนก่อนลบ
before_count = spark.sql("SELECT count(*) FROM local.db.healthcare_table").collect()[0][0]

# ใช้คำสั่ง DELETE เหมือนใน Database ทั่วไป
spark.sql("DELETE FROM local.db.healthcare_table WHERE Name = 'Bobby JacksOn'")

# ตรวจสอบจำนวนหลังลบ
after_count = spark.sql("SELECT count(*) FROM local.db.healthcare_table").collect()[0][0]

print(f"Count Before: {before_count}")
print(f"Count After: {after_count}")
print(f"Deleted: {before_count - after_count} records")

print("--- Step 4: Verification ---")
# ดูประวัติการเปลี่ยนแปลง (Snapshot) ของ Iceberg
print("--- Table History (Snapshots) ---")
spark.sql("SELECT committed_at, snapshot_id, operation FROM local.db.healthcare_table.snapshots").show()

print("✅ Job Set 2 (Iceberg) Finished Successfully!")
spark.stop()