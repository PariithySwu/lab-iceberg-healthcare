import os
import sys

java_path = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot" 

os.environ["JAVA_HOME"] = java_path
os.environ["PATH"] = os.path.join(java_path, "bin") + ";" + os.environ["PATH"]

from pyspark.sql import SparkSession
import os

# 1. สร้าง Spark Session
spark = SparkSession.builder \
    .appName("Healthcare_Parquet_Job") \
    .getOrCreate()

# กำหนด Path สำหรับอ่านและเขียนข้อมูล
input_path = "data/healthcare_dataset.csv"
output_path = "output/healthcare_parquet"

print("--- Step 1: Loading Data from CSV ---")

if os.path.exists(input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.show(5)
    
    print("--- Step 2: Deleting Record ---")
    df_cleaned = df.filter(df.Name != "Bobby JacksOn")
    
    print(f"Original: {df.count()} | Remaining: {df_cleaned.count()}")

    print("--- Step 3: Saving to Parquet ---")
    df_cleaned.write.mode("overwrite").parquet(output_path)
    print("--- Job Set 1 Finished! ---")
else:
    print(f"!!! Error: File not found at {input_path} !!!")

spark.stop()