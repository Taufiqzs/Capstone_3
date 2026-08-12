"""Mengorkestrasi pipeline batch dan data warehouse NYC Green Taxi milik Taufiq.

Lokasi file yang diharapkan pada repository instruktur:
``dags/submissions/taufiqzahrus/taufiqzahrus_green_taxi_pipeline.py``

DAG menggunakan koneksi GCP pada Airflow atau service account yang terpasang
pada VM. DAG tidak pernah membaca file JSON key service account yang diunduh.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# STUDENT_ID adalah identitas resmi peserta dan label kepemilikan di Airflow.
STUDENT_ID = "taufiqzahrus"
# PROJECT_ID menentukan Google Cloud project yang menampung seluruh resource pipeline.
PROJECT_ID = "jcdeah-009"
# REGION menyelaraskan lokasi operasi BigQuery, dataset, dan bucket Cloud Storage.
REGION = "asia-southeast2"
# BUCKET menyimpan objek Parquet April–Mei sebelum dimuat oleh BigQuery.
BUCKET = "taufiqzahrus-capstone3"
# DATASET mengelompokkan tabel staging, intermediate, dan mart di BigQuery.
DATASET = "taufiqzahrus_capstone3"
# APRIL_OBJECT adalah path GCS yang diwajibkan untuk file batch April 2025.
APRIL_OBJECT = "raw/green_tripdata_2025-04.parquet"
# MAY_OBJECT adalah path GCS yang diwajibkan untuk file batch Mei 2025.
MAY_OBJECT = "raw/green_tripdata_2025-05.parquet"
# SENSOR_POKE_INTERVAL_SECONDS mengatur frekuensi sensor memeriksa GCS.
SENSOR_POKE_INTERVAL_SECONDS = 30
# SENSOR_TIMEOUT_SECONDS mencegah sensor menunggu tanpa batas saat objek tidak ada.
SENSOR_TIMEOUT_SECONDS = 600
# RETRY_COUNT memberi operasi cloud dua percobaan tambahan jika terjadi error sementara.
RETRY_COUNT = 2
# RETRY_DELAY_MINUTES menentukan jeda antarpercobaan ulang task.
RETRY_DELAY_MINUTES = 5

# EXPECTED_INTERMEDIATE_COLUMNS mendeteksi kolom hilang atau perubahan nama schema.
EXPECTED_INTERMEDIATE_COLUMNS = 15
# DAG_ID adalah identifier unik Airflow untuk submission milik Taufiq.



def qualified_table(table_name: str) -> str:
    """Membentuk referensi tabel BigQuery lengkap yang diapit backtick.

    Args:
        table_name: Nama tabel di dalam dataset BigQuery milik peserta.

    Returns:
        Identifier Standard SQL dalam format ``project.dataset.table``.
    """

    # table_reference mencegah pengulangan identifier project dan dataset pada SQL.
    table_reference = f"`{PROJECT_ID}.{DATASET}.{table_name}`"
    return table_reference


# INTERMEDIATE_SQL menyatukan data batch dan streaming, memvalidasi, lalu
# menghapus duplikatnya.
INTERMEDIATE_SQL = f"""
-- Mengganti tabel intermediate secara atomik agar rerun tidak menambah duplikat.
CREATE OR REPLACE TABLE {qualified_table('int_green_taxi_trips')}
-- Membuat partisi berdasarkan tanggal bisnis agar pemindaian April–Juli lebih hemat.
PARTITION BY pickup_date
-- Membuat cluster berdasarkan dimensi sumber, lokasi, dan pembayaran.
CLUSTER BY source_type, pickup_location_id, payment_type AS
WITH unified AS (
  -- Menyeragamkan kolom Parquet April–Mei ke kontrak schema warehouse.
  SELECT
    -- trip_id adalah fingerprint deterministik record batch untuk deduplikasi.
    TO_HEX(SHA256(CONCAT(
      CAST(lpep_pickup_datetime AS STRING), '|',
      CAST(lpep_dropoff_datetime AS STRING), '|',
      CAST(PULocationID AS STRING), '|',
      CAST(DOLocationID AS STRING), '|',
      CAST(trip_distance AS STRING), '|',
      CAST(total_amount AS STRING)
    ))) AS trip_id,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    DATE(lpep_pickup_datetime) AS pickup_date,
    CAST(PULocationID AS INT64) AS pickup_location_id,
    CAST(DOLocationID AS INT64) AS dropoff_location_id,
    COALESCE(CAST(passenger_count AS INT64), 1) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(tip_amount AS FLOAT64) AS tip_amount,
    CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    CAST(total_amount AS FLOAT64) AS total_amount,
    CAST(payment_type AS INT64) AS payment_type,
    'batch' AS source_type,
    CURRENT_TIMESTAMP() AS ingestion_time
  FROM {qualified_table('stg_green_taxi_batch')}
  -- Mempertahankan periode resmi April–Mei dan nilai domain bisnis yang valid.
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2025-04-01' AND '2025-05-31'
    AND lpep_dropoff_datetime > lpep_pickup_datetime
    AND PULocationID BETWEEN 1 AND 265
    AND DOLocationID BETWEEN 1 AND 265
    AND trip_distance > 0
    AND fare_amount >= 0
    AND total_amount > 0

  UNION ALL

  -- Memilih event Juni–Juli yang telah divalidasi Dataflow dengan schema yang sama.
  SELECT
    event_id AS trip_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    payment_type,
    'stream' AS source_type,
    ingestion_time
  FROM {qualified_table('stg_green_taxi_stream')}
  -- Mencegah baris upstream bermasalah masuk ke periode selain Juni–Juli.
  WHERE pickup_date BETWEEN '2025-06-01' AND '2025-07-31'
), ranked AS (
  -- row_number=1 mempertahankan salinan terbaru untuk setiap trip_id yang sama.
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY trip_id
      ORDER BY ingestion_time DESC
    ) AS row_number
  FROM unified
)
SELECT * EXCEPT(row_number)
FROM ranked
WHERE row_number = 1
"""

# MART_SQL membangun ulang ringkasan analitik harian dan bulanan dari data bersih.
MART_SQL = f"""
-- Mart harian mendukung dashboard tren dan perbandingan batch dengan stream.
CREATE OR REPLACE TABLE {qualified_table('mart_daily_taxi_summary')}
PARTITION BY pickup_date
CLUSTER BY source_type AS
SELECT
  pickup_date,
  source_type,
  COUNT(*) AS trip_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS average_total_amount,
  ROUND(AVG(trip_distance), 2) AS average_trip_distance,
  ROUND(
    AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND)) / 60,
    2
  ) AS average_duration_minutes
FROM {qualified_table('int_green_taxi_trips')}
GROUP BY pickup_date, source_type;

-- Mart bulanan mendukung ringkasan kinerja April hingga Juli.
CREATE OR REPLACE TABLE {qualified_table('mart_monthly_taxi_performance')} AS
SELECT
  DATE_TRUNC(pickup_date, MONTH) AS pickup_month,
  source_type,
  COUNT(*) AS trip_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS average_total_amount,
  ROUND(AVG(trip_distance), 2) AS average_trip_distance
FROM {qualified_table('int_green_taxi_trips')}
GROUP BY pickup_month, source_type
"""

# default_args menerapkan kepemilikan dan mekanisme retry pada seluruh task DAG.
default_args = {
    "owner": STUDENT_ID,
    "depends_on_past": False,
    "retries": RETRY_COUNT,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
}

# dag adalah container workflow yang ditampilkan pada antarmuka Airflow instruktur.
with DAG(
    dag_id="taufiqzahrus_green_taxi_pipeline",
    description=(
        "Pemuatan batch NYC Green Taxi dan transformasi data warehouse terpadu"
    ),
    default_args=default_args,
    start_date=datetime(2025, 4, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["capstone-3", STUDENT_ID, "batch", "streaming"],
) as dag:
    # create_dataset memastikan dataset tersedia sebelum operasi tabel dimulai.
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_if_missing",
        project_id=PROJECT_ID,
        dataset_id=DATASET,
        location=REGION,
        exists_ok=True,
    )

    # wait_for_april menunggu secara efisien sampai objek Parquet April tersedia.
    wait_for_april = GCSObjectExistenceSensor(
        task_id="wait_for_april_parquet",
        bucket=BUCKET,
        object=APRIL_OBJECT,
        poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    )

    # wait_for_may menunggu secara efisien sampai objek Parquet Mei tersedia.
    wait_for_may = GCSObjectExistenceSensor(
        task_id="wait_for_may_parquet",
        bucket=BUCKET,
        object=MAY_OBJECT,
        poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    )

    # load_batch_staging memuat dua file bulanan ke satu tabel staging.
    load_batch_staging = GCSToBigQueryOperator(
        task_id="load_april_may_to_batch_staging",
        bucket=BUCKET,
        source_objects=[APRIL_OBJECT, MAY_OBJECT],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET}.stg_green_taxi_batch"
        ),
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "lpep_pickup_datetime"},
        cluster_fields=["PULocationID", "payment_type"],
        location=REGION,
    )

    # check_batch_row_count menggagalkan pipeline jika tabel staging kosong.
    check_batch_row_count = BigQueryCheckOperator(
        task_id="check_batch_row_count",
        sql=(
            "SELECT COUNT(*) > 0 FROM "
            f"{qualified_table('stg_green_taxi_batch')}"
        ),
        use_legacy_sql=False,
        location=REGION,
    )

    # check_batch_required_nulls memastikan timestamp dan ID zona wajib tidak NULL.
    check_batch_required_nulls = BigQueryCheckOperator(
        task_id="check_batch_required_nulls",
        sql=f"""
        SELECT COUNTIF(
          lpep_pickup_datetime IS NULL
          OR lpep_dropoff_datetime IS NULL
          OR PULocationID IS NULL
          OR DOLocationID IS NULL
        ) = 0
        FROM {qualified_table('stg_green_taxi_batch')}
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    # transform_intermediate menjalankan SQL penyatuan schema dan deduplikasi.
    transform_intermediate = BigQueryInsertJobOperator(
        task_id="transform_to_intermediate",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": INTERMEDIATE_SQL,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # check_required_columns memverifikasi kontrak kolom tabel intermediate.
    check_required_columns = BigQueryCheckOperator(
        task_id="check_required_schema",
        sql=f"""
        SELECT COUNT(*) = {EXPECTED_INTERMEDIATE_COLUMNS}
        FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'int_green_taxi_trips'
          AND column_name IN (
            'trip_id', 'pickup_datetime', 'dropoff_datetime', 'pickup_date',
            'pickup_location_id', 'dropoff_location_id', 'passenger_count',
            'trip_distance', 'fare_amount', 'tip_amount', 'tolls_amount',
            'total_amount', 'payment_type', 'source_type', 'ingestion_time'
          )
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    # check_invalid_values memvalidasi ulang aturan bisnis setelah kedua sumber digabung.
    check_invalid_values = BigQueryCheckOperator(
        task_id="check_invalid_values",
        sql=f"""
        SELECT COUNTIF(
          pickup_datetime >= dropoff_datetime
          OR trip_distance <= 0
          OR fare_amount < 0
          OR total_amount <= 0
          OR pickup_location_id NOT BETWEEN 1 AND 265
          OR dropoff_location_id NOT BETWEEN 1 AND 265
          OR source_type NOT IN ('batch', 'stream')
        ) = 0
        FROM {qualified_table('int_green_taxi_trips')}
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    # check_duplicates memastikan hanya ada satu baris untuk setiap trip_id.
    check_duplicates = BigQueryCheckOperator(
        task_id="check_duplicates",
        sql=f"""
        SELECT COUNT(*) = COUNT(DISTINCT trip_id)
        FROM {qualified_table('int_green_taxi_trips')}
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    # Memastikan data hasil streaming tersedia untuk periode proyek.
    check_stream_data_exists = BigQueryCheckOperator(
        task_id="check_stream_data_exists",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM {qualified_table('stg_green_taxi_stream')}
            WHERE pickup_date BETWEEN '2025-06-01' AND '2025-07-31'
            AND source_type = 'stream'
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    # build_marts membuat tabel agregat harian dan bulanan yang siap dianalisis.
    build_marts = BigQueryInsertJobOperator(
        task_id="build_analytical_marts",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": MARTS_SQL,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # check_mart_row_count memastikan mart harian menghasilkan data analitik.
    check_mart_row_count = BigQueryCheckOperator(
        task_id="check_mart_row_count",
        sql=(
            "SELECT COUNT(*) > 0 FROM "
            f"{qualified_table('mart_daily_taxi_summary')}"
        ),
        use_legacy_sql=False,
        location=REGION,
    )

    # Dataset harus tersedia sebelum dua sensor GCS mulai bekerja secara paralel.
    create_dataset >> [wait_for_april, wait_for_may]
    # Kedua objek sumber harus tersedia sebelum pemuatan batch dimulai.
    [wait_for_april, wait_for_may] >> load_batch_staging
    # Pemeriksaan jumlah baris dan NULL berjalan paralel setelah staging berhasil.
    load_batch_staging >> [check_batch_row_count, check_batch_required_nulls]
    # Transformasi intermediate dimulai hanya jika dua pemeriksaan staging lulus.
    [check_batch_row_count, check_batch_required_nulls] >> transform_intermediate
    # Pemeriksaan schema, nilai, keunikan, dan freshness berjalan secara paralel.
    transform_intermediate >> [
        check_required_columns,
        check_invalid_values,
        check_duplicates,
        check_stream_data_exists,
    ]
    # Seluruh quality gate harus lulus sebelum mart dan validasi akhir dijalankan.
    [
        check_required_columns,
        check_invalid_values,
        check_duplicates,
        check_stream_data_exists,
    ] >> build_marts >> check_mart_row_count