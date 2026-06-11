from pyspark.sql import SparkSession
import os
from pathlib import Path

def load_env(path=".env"):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)

load_env()

PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DB = os.environ.get("POSTGRES_DB", "quantumlane")

spark = (
    SparkSession.builder
    .appName("quantumlane-explore")
    .config("spark.jars", "spark/jars/postgresql-42.7.4.jar")
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://localhost:5432/{PG_DB}")
    .option("dbtable", "realtime.trip_updates")
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"rows: {df.count()}")
df.printSchema()
df.show(5, truncate=False)