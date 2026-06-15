import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_env(path=".env"):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)


load_env()

spark = (
    SparkSession.builder
    .appName("quantumlane-headway")
    .config("spark.jars", "spark/jars/postgresql-42.7.4.jar")
    .config("spark.driver.memory", "4g")          
    .config("spark.sql.shuffle.partitions", "16") 
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://localhost:5432/quantumlane"
conn_props = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

# 1. Get partition bounds dynamically with a cheap pushed-down aggregate query.
# Freeze the read window slightly behind "now" so ingestion's live writes
# don't pile into the last partition mid-read.
bounds_q = """(
    SELECT min(received_at) AS lo,
           max(received_at) AS hi
    FROM realtime.trip_updates
    WHERE received_at < now() - interval '1 minute'
) AS b"""
bounds = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", bounds_q)
    .options(**conn_props)
    .load()
    .first()
)
lo, hi = str(bounds["lo"]), str(bounds["hi"])
print(f"bounds: {lo} .. {hi}")

# 2. Partitioned read using the live bounds.
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "realtime.trip_updates")
    .option("partitionColumn", "received_at")
    .option("lowerBound", lo)
    .option("upperBound", hi)
    .option("numPartitions", "4")
    .options(**conn_props)
    .load()
)

# 3. Collapse the feed to ONE arrival per (trip, stop): the latest prediction we saw.
#    The feed re-reports the same trip+stop on every poll; we want one row per actual
#    scheduled vehicle visit, taking the most recent prediction of its arrival.
arrivals = (
    df.filter(F.col("arrival_time").isNotNull())
    .groupBy("route_id", "direction_id", "stop_id", "trip_id")
    .agg(F.max("arrival_time").alias("arrival_time"))
)

# 4. Window: order vehicles by arrival within each route+stop.
#    direction_id is always NULL in the TTC feed — dropped (it grouped nothing).
#    Still partition by stop, because headway is measured BETWEEN vehicles AT a stop;
#    we aggregate UP to route level afterward.
w = Window.partitionBy("route_id", "direction_id", "stop_id").orderBy("arrival_time")

# 5. Per-stop headways (same as before, minus direction).
headways = (
    arrivals
    .withColumn("prev_arrival", F.lag("arrival_time").over(w))
    .withColumn(
        "headway_s",
        F.col("arrival_time").cast("long") - F.col("prev_arrival").cast("long"),
    )
    .filter(F.col("headway_s").isNotNull())
    .filter((F.col("headway_s") > 30) & (F.col("headway_s") < 7200))
)

# 6. Aggregate UP to route level — one reliability row per route.
route_reliability = (
    headways.groupBy("route_id", "direction_id")
    .agg(
        F.count("*").alias("n_gaps"),
        F.countDistinct("stop_id").alias("n_stops"),
        F.round(F.avg("headway_s") / 60, 1).alias("avg_headway_min"),
        F.round(F.stddev("headway_s") / 60, 1).alias("stddev_headway_min"),
        F.round(F.stddev("headway_s") / F.avg("headway_s"), 2).alias("headway_cov"),
    )
    .filter(F.col("n_gaps") >= 30)
    .orderBy(F.col("headway_cov").desc())
)

route_reliability.show(30, truncate=False)

input("Spark UI at http://localhost:4040 — press Enter to exit...")