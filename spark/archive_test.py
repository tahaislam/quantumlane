"""
Standalone test of the daily feed archival logic — NO Dagster.

Tests the full cycle in isolation: read one UTC day from Postgres -> Parquet
-> upload to S3 -> read back and verify row count. Once this works, the same
logic graduates into a Dagster asset (scheduling tested separately).

Run:
    source .venv-spark/bin/activate   # or any venv with the deps below
    pip install pandas pyarrow boto3 sqlalchemy psycopg2-binary
    python spark/archive_test.py                 # archives yesterday (UTC)
    python spark/archive_test.py 2026-06-13      # archives a specific day

Reads DB creds + S3 config from the project .env (same vars as the Spark script).
Writes to the real S3 bucket (that's where the data goes anyway).
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from sqlalchemy import create_engine


FEEDS = {
    "trip_updates": "received_at",
    "vehicle_positions": "received_at",
}


def load_env(path=".env"):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)


def postgres_url():
    # Adjust var names to match your .env (grep -i postgres .env).
    user = os.environ["POSTGRES_USER"]
    pw = os.environ["POSTGRES_PASSWORD"]
    db = os.environ.get("POSTGRES_DB", "quantumlane")
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


def resolve_day(argv):
    """Day to archive: CLI arg (YYYY-MM-DD) or default to yesterday UTC."""
    if len(argv) > 1:
        return datetime.strptime(argv[1], "%Y-%m-%d").date()
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


def archive_day(target_day):
    day_start = datetime.combine(target_day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    engine = create_engine(
        postgres_url(),
        execution_options={"stream_results": True}
    )
    s3 = boto3.client(
        "s3",
        region_name=os.environ["QL_S3_REGION"],
        aws_access_key_id=os.environ["QL_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["QL_AWS_SECRET_ACCESS_KEY"],
    )
    bucket = os.environ["QL_S3_BUCKET"]

    print(f"Archiving {target_day} (UTC window {day_start} .. {day_end})\n")

    for table, ts_col in FEEDS.items():
        query = f"""
            SELECT * FROM realtime.{table}
            WHERE {ts_col} >= %(start)s AND {ts_col} < %(end)s
            ORDER BY {ts_col}
        """

        # Write to a LOCAL temp Parquet file incrementally, one chunk at a time.
        local_path = f"/tmp/{table}_{target_day.isoformat()}.parquet"
        writer = None
        total_rows = 0

        chunk_iter = pd.read_sql(
            query, engine,
            params={"start": day_start, "end": day_end},
            chunksize=200_000,          # rows per chunk — tune to memory
        )

        for chunk_df in chunk_iter:
            arrow_chunk = pa.Table.from_pandas(chunk_df, preserve_index=False)
            if writer is None:
                # First chunk defines the schema; open the writer with it.
                writer = pq.ParquetWriter(local_path, arrow_chunk.schema, compression="zstd")
            writer.write_table(arrow_chunk)
            total_rows += len(chunk_df)
            print(f"  {table}: wrote chunk of {len(chunk_df):,} (running {total_rows:,})")

        if writer is None:
            print(f"  {table}: 0 rows for {target_day} — skipping")
            continue
        writer.close()

        # Upload the finished single file to S3, then remove the local temp.
        key = f"{table}/dt={target_day.isoformat()}/part-0.parquet"
        s3.upload_file(local_path, bucket, key)
        parquet_size = os.path.getsize(local_path)
        os.remove(local_path)

        print(
            f"  {table}: {total_rows:,} rows -> s3://{bucket}/{key} "
            f"({parquet_size/1e6:.2f} MB, zstd)\n"
        )

    return bucket


def verify_readback(bucket, target_day):
    print("Verifying read-back from S3:")
    storage_options = {
        "key": os.environ["QL_AWS_ACCESS_KEY_ID"],
        "secret": os.environ["QL_AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"region_name": os.environ["QL_S3_REGION"]},
    }
    for table in FEEDS:
        uri = f"s3://{bucket}/{table}/dt={target_day.isoformat()}/part-0.parquet"
        try:
            df = pd.read_parquet(uri, storage_options=storage_options)
            print(f"  {table}: {len(df):,} rows, {len(df.columns)} cols — OK")
        except Exception as e:  # noqa: BLE001
            print(f"  {table}: read-back FAILED — {e}")


if __name__ == "__main__":
    load_env()
    day = resolve_day(sys.argv)
    bucket = archive_day(day)
    print()
    verify_readback(bucket, day)