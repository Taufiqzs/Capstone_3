# Referensi Kode

Dokumen ini memetakan implementasi Capstone Project 3 ke source file, menjelaskan lokasi setiap perilaku utama, mencatat referensi dokumentasi resmi, dan memperjelas hubungan antara repositori Capstone dengan runtime Airflow terpisah.

## 1. Struktur Dua Repositori

| Repositori | Branch | Fungsi |
|---|---|---|
| `Taufiqzs/airflow-capstone-project` | `taufiq-capstone3` | Runtime Docker/Airflow, konfigurasi Compose, dan DAG submission |
| `Capstone_3_Taufiq` | Branch utama proyek | Dataflow, publisher, script GCP, schema, SQL, tests, dokumentasi, dan README mentor |

Branch Airflow:

```text
https://github.com/Taufiqzs/airflow-capstone-project/tree/taufiq-capstone3
```

Kedua repositori harus di-clone sebagai folder sibling. Dockerfile dan `docker-compose.yml` tidak perlu dipindahkan atau diduplikasi ke repositori Capstone.

## 2. Peta Kode Internal

| Area | File utama | Implementasi utama |
|---|---|---|
| Orkestrasi batch | `dags/submissions/taufiqzahrus/taufiqzahrus_green_taxi_pipeline.py` pada branch Airflow | DAG, GCS sensor, load Parquet, transformasi BigQuery, quality check, dan dependency task |
| Pipeline streaming | `dataflow/streaming_pipeline.py` | Input Pub/Sub, Beam side outputs, serta penulisan valid/dead-letter ke BigQuery |
| Validasi streaming | `dataflow/transforms.py` | Parsing JSON, pemeriksaan timestamp/domain, enrichment, dan pembuatan dead-letter row |
| Generator event | `publisher/event_generator.py` | Simulasi event Green Taxi Juni–Juli 2025 yang dapat diuji secara deterministik |
| Publisher Pub/Sub | `publisher/publish_green_taxi.py` | CLI, rate control, asynchronous publishing, graceful shutdown, dan dry run |
| Setup cloud | `scripts/setup_gcp.sh` | API, GCS bucket, lifecycle rule, BigQuery dataset, topic/subscription Pub/Sub |
| Peluncuran Dataflow | `scripts/run_dataflow.sh` | DataflowRunner options, staging/temp path, subscription input, output table, dan worker limit |
| Ingestion batch | `scripts/upload_batch_to_gcs.py` | Download resmi NYC TLC, transfer lokal atomik, dan upload GCS idempotent |
| Kontrak event | `schemas/green_taxi_event.schema.json` | Field, required properties, type, range, dan enum pada JSON Schema |
| Model unified | `sql/transform/01_intermediate.sql` | Penyelarasan schema batch/stream, business filter, lineage, ID SHA-256/UUID, dan deduplikasi |
| Data mart | `sql/transform/02_marts.sql` | Tabel agregasi harian dan bulanan |
| Analisis batch | `sql/analytics/01_batch_monthly_summary.sql` | Query performa bulanan April–Mei |
| Perbandingan sumber | `sql/analytics/02_batch_vs_stream.sql` | Query performa tertimbang batch dan stream |
| Unit test generator | `tests/test_event_generator.py` | Test domain generator dan serialisasi JSON |
| Unit test streaming | `tests/test_streaming_validation.py` | Test enrichment valid dan jalur invalid/dead-letter |

## 3. Data Lineage Berdasarkan Lokasi Kode

| Tahap | Input | Output | Definisi |
|---|---|---|---|
| Download sumber batch | Parquet publik NYC TLC | File lokal sementara | `scripts/upload_batch_to_gcs.py` |
| Landing batch | Parquet lokal | `gs://taufiqzahrus-capstone3/raw/...` | `scripts/upload_batch_to_gcs.py` |
| Staging batch | Dua objek Parquet GCS | `stg_green_taxi_batch` | Task Airflow `load_april_may_to_batch_staging` |
| Generate stream | Python RNG dan domain constants | JSON event taxi | `publisher/event_generator.py` |
| Transport stream | JSON event taxi | Topic Pub/Sub | `publisher/publish_green_taxi.py` |
| Pemrosesan stream | Subscription Pub/Sub | Beam valid dan invalid side outputs | `dataflow/streaming_pipeline.py` dan `dataflow/transforms.py` |
| Staging stream | Valid output | `stg_green_taxi_stream` | Beam `WriteValidBigQuery` |
| Dead-letter | Invalid output | `stg_green_taxi_stream_dead_letter` | Beam `WriteInvalidBigQuery` |
| Standardisasi | Staging batch dan stream | `int_green_taxi_trips` | DAG `INTERMEDIATE_SQL` dan `sql/transform/01_intermediate.sql` |
| Agregasi | Unified intermediate | Mart harian dan bulanan | DAG `MART_SQL` dan `sql/transform/02_marts.sql` |

## 4. Aturan Bisnis dan Validasi

| Aturan | Implementasi |
|---|---|
| Periode batch adalah April–Mei 2025 | Date filter pada `INTERMEDIATE_SQL` |
| Periode stream adalah Juni–Juli 2025 | Batas generator dan `validate_and_transform()` |
| Waktu drop-off setelah pickup | Streaming transform dan intermediate SQL |
| Taxi zone ID berada pada 1–265 | Generator, streaming transform, intermediate SQL, dan quality check Airflow |
| Passenger count berada pada 1–6 | Generator dan streaming transform |
| Payment type termasuk 1–4 | Generator, JSON Schema, dan streaming transform |
| Distance dan total amount bernilai positif | Streaming transform, intermediate SQL, dan quality check Airflow |
| Fare, tip, dan toll tidak negatif | Streaming transform; SQL batch memvalidasi fare dan total |
| Label sumber stream harus `stream` | Generator, JSON Schema, dan streaming transform |
| Duplicate `trip_id` dihapus | `ROW_NUMBER()` per `trip_id`; ingestion terbaru dipertahankan |
| Invalid stream tetap dapat diaudit | Dead-letter output berisi raw payload, error reason, dan processing time |

## 5. Referensi Konfigurasi

| Variable | Nilai default | Digunakan oleh |
|---|---|---|
| `GCP_PROJECT_ID` | `jcdeah-009` | Setup, uploader, publisher, dan Dataflow |
| `GCP_REGION` | `asia-southeast2` | Setup dan Dataflow |
| `STUDENT_ID` | `taufiqzahrus` | Setup dan penamaan job Dataflow |
| `GCS_BUCKET` | `taufiqzahrus-capstone3` | Setup, uploader, dan Dataflow staging |
| `BIGQUERY_DATASET` | `taufiqzahrus_capstone3` | Setup dan output Dataflow |
| `PUBSUB_TOPIC` | `taufiqzahrus-green-taxi-events` | Setup dan publisher |
| `PUBSUB_SUBSCRIPTION` | `taufiqzahrus-green-taxi-dataflow` | Setup dan Dataflow |
| `EVENT_RATE` | `5` | Publisher |
| `EVENT_COUNT` | `100` | Publisher |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-api-server:8080/execution/` | Runtime Airflow 3 pada Compose |
| `GOOGLE_CLOUD_PROJECT` | `jcdeah-009` | Runtime Airflow dan Google provider |

DAG dan standalone SQL masih memuat sebagian project/resource ID secara langsung. Environment variables pada setup, uploader, publisher, dan Dataflow belum sepenuhnya menggantikan konstanta di DAG/SQL.

## 6. Referensi Eksternal Resmi

### Data sumber

- [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — deskripsi dan distribusi resmi data perjalanan TLC.
- [Direktori Parquet NYC TLC](https://d37ci6vzurychx.cloudfront.net/trip-data/) — sumber yang digunakan `upload_batch_to_gcs.py`.

### Apache Airflow

- [Airflow DAG authoring](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) — struktur DAG, scheduling, dependency, dan perilaku task.
- [Google Cloud Storage operators and sensors](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/gcs.html) — sensing dan operasi objek GCS.
- [Google Cloud BigQuery operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html) — query job, dataset, dan operator validasi.
- [Airflow executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html) — konteks executor/runtime Airflow 3.

### Google Cloud Storage

- [Cloud Storage Python client](https://cloud.google.com/python/docs/reference/storage/latest) — client, bucket, blob, existence check, dan upload.
- [Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle) — lifecycle rule pada `setup_gcp.sh`.
- [Uniform bucket-level access](https://cloud.google.com/storage/docs/uniform-bucket-level-access) — access model bucket.

### Pub/Sub

- [Publish messages with Python](https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library) — asynchronous publisher pattern.
- [Subscriptions overview](https://cloud.google.com/pubsub/docs/subscriber) — pull subscription yang digunakan Dataflow.
- [Pub/Sub message retention](https://cloud.google.com/pubsub/docs/subscription-message-retention) — retensi tujuh hari pada setup script.

### Apache Beam dan Dataflow

- [Apache Beam programming guide](https://beam.apache.org/documentation/programming-guide/) — pipeline, transform, `ParDo`, `DoFn`, dan additional outputs.
- [ReadFromPubSub](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html) — Beam Pub/Sub source.
- [WriteToBigQuery](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html) — penulisan tabel valid dan dead-letter.
- [Run a Beam pipeline on Dataflow](https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options) — runner, project, region, staging, temp location, dan worker options.
- [Dataflow Streaming Engine](https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#streaming-engine) — streaming engine yang diaktifkan launch script.

### BigQuery

- [Load Parquet from Cloud Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet) — batch staging dari GCS.
- [Create partitioned tables](https://cloud.google.com/bigquery/docs/creating-partitioned-tables) — date partitioning pada staging, intermediate, dan mart.
- [Clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables) — clustering pada warehouse.
- [GoogleSQL hash functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions) — fingerprint SHA-256 untuk `trip_id` batch.
- [Numbering functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions) — deduplikasi dengan `ROW_NUMBER()`.
- [INFORMATION_SCHEMA.COLUMNS](https://cloud.google.com/bigquery/docs/information-schema-columns) — schema quality gate Airflow.

### Python dan schema validation

- [Python `dataclasses`](https://docs.python.org/3/library/dataclasses.html) — model immutable `TaxiEvent`.
- [Python `argparse`](https://docs.python.org/3/library/argparse.html) — CLI publisher dan Dataflow.
- [Python `datetime`](https://docs.python.org/3/library/datetime.html) — timezone-aware timestamp dan normalisasi UTC.
- [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12) — dialect kontrak event.
- [pytest](https://docs.pytest.org/en/stable/) — test runner pada `requirements.txt`.

## 7. Catatan Konsistensi Referensi

1. `INTERMEDIATE_SQL` dan `MART_SQL` di dalam DAG menduplikasi standalone SQL. Perubahan logika harus diterapkan pada kedua lokasi sampai refactor dilakukan.
2. JSON Schema masih berfungsi sebagai kontrak dokumentasi; `dataflow/transforms.py` melakukan validasi manual dan belum membaca file schema.
3. `requirements.txt` tidak memuat Airflow atau Google provider karena dependency tersebut merupakan bagian dari runtime Airflow pada repositori terpisah.
4. `setup.py` digunakan untuk packaging worker Dataflow, tetapi metadata dan `install_requires` masih minimal; `run_dataflow.sh` juga mengirim `requirements.txt`.
5. README harus mencantumkan URL kedua repositori dan branch `taufiq-capstone3` secara eksplisit.
6. Gunakan `python3` untuk perintah WSL karena alias `python` tidak tersedia pada environment pengguna.

## 8. Referensi Commit

### Judul commit

```text
feat: complete NYC Green Taxi batch and streaming pipeline
```

### Deskripsi commit bahasa Indonesia

```text
Menyelesaikan pipeline data NYC Green Taxi secara end-to-end di GCP.

Perubahan:
- Menambahkan orkestrasi Airflow, pipeline streaming Dataflow, dan publisher Pub/Sub.
- Menambahkan script setup GCP, upload batch, schema, SQL analitik, dan unit test.
- Menambahkan panduan mentor, referensi kode, serta laporan validasi dalam bahasa Indonesia.
- Menambahkan bukti eksekusi GCS, Pub/Sub, Dataflow, Airflow, dan BigQuery.
- Mengecualikan credential, cache, log, dan generated artifacts dari Git.
```

Perintah commit:

```bash
git commit \
  -m "feat: complete NYC Green Taxi batch and streaming pipeline" \
  -m "Add Airflow orchestration, Dataflow streaming transformations, Pub/Sub publisher, GCP setup scripts, validation evidence, and Indonesian mentor documentation."
```
