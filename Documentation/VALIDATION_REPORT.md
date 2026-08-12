# Laporan Validasi

## 1. Ringkasan Penilaian

**Proyek:** Pipeline Batch dan Streaming NYC Green Taxi

**Pemilik:** Taufiq Zahrus (`taufiqzahrus`)

**Tanggal penilaian:** 12 Agustus 2026

**Repositori proyek:** `Capstone_3_Taufiq`

**Runtime Airflow:** fork `airflow-capstone-project`, branch `taufiq-capstone3`

Proyek ini menunjukkan arsitektur data batch dan streaming yang terintegrasi di Google Cloud. Implementasi mencakup ingestion, orkestrasi, validasi, dead-letter handling, standardisasi, deduplikasi, data mart analitis, serta bukti eksekusi cloud. Screenshot dan hasil eksekusi yang tersedia mendukung keberhasilan penggunaan Airflow, Dataflow, Pub/Sub, Cloud Storage, dan BigQuery.

Proyek sengaja dibagi menjadi dua repositori:

1. `airflow-capstone-project` branch `taufiq-capstone3` menyediakan runtime Docker/Airflow dan DAG submission.
2. `Capstone_3_Taufiq` menyediakan source code Dataflow, publisher, SQL, schema, tests, script GCP, dan dokumentasi.

Karena itu, Dockerfile dan konfigurasi Compose tidak perlu diduplikasi ke repositori Capstone. Mentor harus mengunduh kedua repositori sebagai folder sibling untuk menjalankan pipeline secara end-to-end.

### Kesimpulan umum

| Dimensi | Status | Ringkasan |
|---|---|---|
| Arsitektur | Lulus | Jalur batch dan streaming bergabung ke model intermediate dan data mart yang konsisten |
| Source code inti | Lulus | Source Python, SQL, schema, shell, serta DAG tersedia di dua repositori terkait |
| Validasi statis | Lulus | Kompilasi Python, sintaks Bash, sintaks JSON, dan dry run publisher berhasil |
| Bukti eksekusi cloud | Lulus berdasarkan bukti | Screenshot dan riwayat run menunjukkan eksekusi end-to-end berhasil |
| Unit test offline | Belum dijalankan ulang | Test tersedia, tetapi runtime audit sebelumnya tidak memiliki `pytest` |
| Reproducibility | Lulus bersyarat | Dapat direproduksi dengan mengunduh dua repositori dan mengikuti README |
| Keamanan | Lulus dengan pembersihan | Tidak ditemukan credential key; `.env` asli tetap tidak boleh diunggah |
| Kebersihan paket | Perlu dijaga | Cache, bytecode, `.egg-info`, dan `.env` harus dikecualikan |
| Dokumentasi | Diperbarui | README Indonesia, referensi kode, laporan validasi, serta panduan mentor tersedia |

## 2. Ruang Lingkup

Audit mencakup:

- Struktur kedua repositori dan aset proyek yang diharapkan.
- Struktur DAG Airflow, operator, urutan task, retry, dan quality gate.
- Generator event streaming dan perilaku publisher Pub/Sub.
- Routing Apache Beam/Dataflow, schema, serta penulisan ke BigQuery.
- Logika validasi dan enrichment.
- Script setup Google Cloud dan upload batch.
- SQL transformasi dan analitik BigQuery.
- Kontrak JSON Schema.
- Dependency dan metadata packaging.
- Definisi unit test.
- Screenshot dokumentasi.
- Pemeriksaan pola credential, identifier peserta lain, dan generated artifacts.
- Keterhubungan branch Airflow `taufiq-capstone3` dengan repositori Capstone.

Audit tidak mengubah atau melakukan query ulang terhadap resource Google Cloud aktif, serta tidak menjalankan ulang environment Airflow dan Dataflow dari awal.

## 3. Metode dan Hasil Validasi

### 3.1 Pemeriksaan source dan struktur proyek

**Hasil: Lulus**

Repositori Capstone berisi area implementasi utama:

- `dataflow/`
- `publisher/`
- `scripts/`
- `schemas/`
- `sql/`
- `tests/`
- `Documentation/`

DAG dan runtime Airflow tersedia pada fork `airflow-capstone-project`, branch `taufiq-capstone3`, khususnya di:

```text
dags/submissions/taufiqzahrus/taufiqzahrus_green_taxi_pipeline.py
```

Tidak ditemukan referensi kode kepada peserta lain seperti `agungnugraha`. Kepemilikan proyek konsisten menggunakan `taufiqzahrus`.

### 3.2 Kompilasi Python

Perintah audit:

```bash
python3 -m compileall -q .
```

**Hasil: Lulus**

Seluruh source Python yang diperiksa dapat dikompilasi tanpa syntax error. Pada WSL pengguna, executable yang tersedia adalah `python3`, bukan `python`.

### 3.3 Sintaks shell

```bash
bash -n scripts/setup_gcp.sh
bash -n scripts/run_dataflow.sh
```

**Hasil: Lulus**

Kedua script lolos validasi sintaks Bash, menggunakan `set -euo pipefail`, dan memvalidasi konfigurasi yang diperlukan sebelum operasi cloud.

### 3.4 Sintaks JSON Schema

```bash
python3 -m json.tool schemas/green_taxi_event.schema.json >/dev/null
```

**Hasil: Lulus**

Schema merupakan JSON valid dan mendeklarasikan JSON Schema Draft 2020-12.

### 3.5 Dry run publisher

```bash
python3 publisher/publish_green_taxi.py --dry-run --count 1 --rate 100
```

**Hasil: Lulus**

Publisher menghasilkan satu objek JSON dengan 15 field sumber dan `source_type = "stream"` tanpa menghubungi Pub/Sub.

### 3.6 Unit test

Perintah yang harus dijalankan:

```bash
python3 -m pytest -q
```

**Hasil audit sebelumnya: Belum dijalankan ulang**

Runtime audit sebelumnya melaporkan `No module named pytest`. File test tersedia, tetapi cache hasil test lama tidak dianggap sebagai bukti bahwa source terbaru lulus. Jalankan kembali setelah menginstal `requirements.txt`.

Cakupan test yang tersedia:

- 500 event deterministik terhadap aturan tanggal dan business domain.
- Serialisasi JSON.
- Enrichment event streaming valid.
- Penolakan event bulan Agustus.
- Penolakan JSON rusak.
- Penolakan JSON yang bukan objek.

### 3.7 Validasi Docker Compose dan branch Airflow

Branch berikut telah berhasil dibuat dan di-push:

```text
https://github.com/Taufiqzs/airflow-capstone-project/tree/taufiq-capstone3
```

Konfigurasi `docker-compose.yml` telah ditambahkan dengan:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "http://airflow-api-server:8080/execution/"
GOOGLE_CLOUD_PROJECT: "jcdeah-009"
```

Perintah validasi:

```bash
docker compose config >/dev/null && echo "Konfigurasi Compose valid"
```

**Hasil: Branch berhasil dipublikasikan; konfigurasi merupakan konfigurasi yang digunakan pada run Airflow yang berhasil.**

### 3.8 Bukti eksekusi cloud

**Hasil: Lulus berdasarkan bukti yang tersedia**

Folder dokumentasi memberikan bukti untuk:

- File raw April dan Mei di Cloud Storage.
- Lifecycle configuration bucket.
- Topic dan subscription Pub/Sub.
- Job Dataflow aktif.
- Routing valid dan dead-letter Dataflow.
- Pengiriman invalid event.
- Tabel valid dan dead-letter di BigQuery.
- Sampel event valid terbaru.
- Satu rejected dead-letter event.
- 110 baris valid dari urutan publish 100 ditambah 10 event.
- Tidak ada duplikasi dan invalid record tidak masuk tabel valid.
- Perbandingan batch/stream serta performa batch bulanan.
- Dua run DAG Airflow yang berhasil.

## 4. Kekuatan yang Terverifikasi

### 4.1 Arsitektur dua jalur yang jelas

Ingestion batch dan streaming dipisahkan, lalu digabungkan ke model intermediate yang terstandarisasi sehingga lineage mudah dilacak dan analisis berdasarkan sumber dapat dilakukan.

### 4.2 Perilaku rerun yang kuat

- Batch staging menggunakan `WRITE_TRUNCATE`.
- Tabel intermediate dan mart menggunakan `CREATE OR REPLACE`.
- `ROW_NUMBER()` mempertahankan record terbaru untuk setiap `trip_id`.
- Script setup GCP dan upload batch memeriksa keberadaan resource atau objek.

### 4.3 Quality gate yang praktis

DAG mencegah pembuatan mart jika pemeriksaan jumlah baris, required fields, schema, business values, duplicate ID, atau ketersediaan data stream gagal. Validasi streaming memisahkan message yang malformed atau di luar domain tanpa menghentikan pipeline.

### 4.4 Dead-letter handling yang dapat diaudit

Message yang ditolak menyimpan raw payload, alasan error, dan waktu pemrosesan.

### 4.5 Desain storage yang mempertimbangkan biaya

Partitioning dan clustering diterapkan pada tabel staging, intermediate, dan mart. Jumlah worker Dataflow dibatasi dan lifecycle rule bucket dikonfigurasi.

### 4.6 Pendekatan credential yang aman

Kode menggunakan Application Default Credentials dan tidak membaca file JSON service account hasil download. Tidak ditemukan private key, access token, API key, atau password dalam source yang diperiksa.

## 5. Temuan dan Rekomendasi

### Prioritas tinggi

#### F-01: Prosedur penggunaan dua repositori harus selalu dijelaskan

**Status terbaru:** Ditangani dalam README.

**Risiko:** Mentor dapat mengira satu repositori saja cukup atau mencoba memindahkan Dockerfile ke repositori Capstone.

**Tindakan:** Pertahankan petunjuk bahwa kedua repositori harus di-clone sebagai folder sibling dan branch Airflow yang digunakan adalah `taufiq-capstone3`. Jangan menduplikasi Dockerfile/Compose ke repositori Capstone.

#### F-02: Unit test terbaru belum direkam

**Dampak:** Source terbaru belum memiliki bukti independen berupa output pytest pada laporan ini.

**Rekomendasi:** Buat virtual environment bersih, instal dependency, jalankan `python3 -m pytest -q`, lalu catat versi Python dan jumlah test yang lulus.

### Prioritas menengah

#### F-03: `.env` asli tidak boleh didistribusikan

Gunakan hanya `.env.example`. Walaupun `.env` saat ini mungkin hanya berisi konfigurasi resource, isinya dapat berubah dan berpotensi memuat secret.

#### F-04: Generated artifacts harus dibersihkan

Jangan commit atau masukkan ke ZIP:

```text
__pycache__/
*.pyc
.pytest_cache/
*.egg-info/
logs/
config/simple_auth_manager_passwords.json.generated
```

#### F-05: Konfigurasi masih terduplikasi dan sebagian hardcoded

Resource ID sudah berbasis environment variable pada script, tetapi sebagian masih tertanam di DAG dan SQL. Gunakan Airflow Variables atau environment variables dan satu sumber konfigurasi bila proyek dikembangkan lebih lanjut.

#### F-06: Logika transformasi SQL terduplikasi

SQL intermediate dan mart berada di dalam DAG serta folder `sql/transform/`. Sebaiknya DAG membaca file SQL version-controlled agar perubahan hanya dilakukan di satu tempat.

### Prioritas rendah

#### F-07: Pesan error validasi masih bercampur bahasa

Gunakan error code stabil seperti `INVALID_JSON_TYPE` dan `OUTSIDE_EVENT_PERIOD`, lalu test berdasarkan code tersebut.

#### F-08: JSON Schema belum digunakan langsung saat runtime

Tambahkan contract consistency test dengan JSON Schema validator agar schema dan validasi Python tidak menyimpang.

#### F-09: `setup.py` masih minimal

Pertimbangkan migrasi ke `pyproject.toml` atau lengkapi metadata, dependency, dan versi Python yang didukung.

#### F-10: Penamaan file dokumentasi perlu konsisten

Gunakan urutan dan format lowercase snake_case, hindari nomor ganda, nomor yang terlewat, ekstensi seperti `.png.jpg`, dan typo seperti `subcription`.

#### F-11: Lifecycle rule mencakup seluruh bucket

Aturan 30 hari juga dapat menghapus raw data dan artifact Dataflow. Dokumentasikan sebagai kebijakan environment capstone sementara atau pisahkan durable raw data dan temporary artifact.

## 6. Checklist Reproducibility untuk Mentor

- [ ] Clone `airflow-capstone-project` branch `taufiq-capstone3`.
- [ ] Clone `Capstone_3_Taufiq` pada parent directory yang sama.
- [ ] Kedua folder menggunakan nama dan struktur sibling sesuai README.
- [ ] Salin `.env.example` menjadi `.env` secara lokal dan isi konfigurasi sendiri.
- [ ] Pastikan `.env` tidak masuk Git.
- [ ] Jalankan `docker compose config` dari repositori Airflow.
- [ ] Jalankan `docker compose build` dan `docker compose up -d`.
- [ ] Pastikan DAG `taufiqzahrus_green_taxi_pipeline` terdeteksi.
- [ ] Instal `requirements.txt` pada environment Capstone.
- [ ] Jalankan `python3 -m pytest -q` dan catat hasilnya.
- [ ] Pastikan dry run publisher menghasilkan JSON valid.
- [ ] Pastikan dua file Parquet sumber tersedia.
- [ ] Demonstrasikan route valid dan dead-letter Dataflow.
- [ ] Jalankan DAG satu kali untuk clean validation run.
- [ ] Pastikan pemeriksaan duplicate menghasilkan nol.
- [ ] Pastikan pemeriksaan invalid value menghasilkan nol.
- [ ] Pastikan kedua mart memiliki coverage sumber/periode yang sesuai.
- [ ] Hentikan job streaming dan container setelah review selesai.

## 7. Query Verifikasi Akhir

### Inventaris tabel

```sql
SELECT
  table_id,
  row_count,
  ROUND(size_bytes / 1024 / 1024, 2) AS size_mb,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `jcdeah-009.taufiqzahrus_capstone3.__TABLES__`
ORDER BY table_id;
```

### Coverage periode dan lineage

```sql
SELECT
  source_type,
  MIN(pickup_date) AS first_date,
  MAX(pickup_date) AS last_date,
  COUNT(*) AS trip_count
FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`
GROUP BY source_type
ORDER BY source_type;
```

Hasil yang diharapkan:

- `batch`: April–Mei 2025.
- `stream`: Juni–Juli 2025.

### Pemeriksaan duplikasi

```sql
SELECT COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_rows
FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`;
```

Hasil yang diharapkan: `0`.

### Pemeriksaan nilai tidak valid

```sql
SELECT COUNT(*) AS invalid_rows
FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`
WHERE pickup_datetime >= dropoff_datetime
   OR trip_distance <= 0
   OR fare_amount < 0
   OR total_amount <= 0
   OR pickup_location_id NOT BETWEEN 1 AND 265
   OR dropoff_location_id NOT BETWEEN 1 AND 265
   OR source_type NOT IN ('batch', 'stream');
```

Hasil yang diharapkan: `0`.

### Rekonsiliasi mart

```sql
WITH intermediate_counts AS (
  SELECT source_type, COUNT(*) AS rows
  FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`
  GROUP BY source_type
),
mart_counts AS (
  SELECT source_type, SUM(trip_count) AS rows
  FROM `jcdeah-009.taufiqzahrus_capstone3.mart_daily_taxi_summary`
  GROUP BY source_type
)
SELECT
  i.source_type,
  i.rows AS intermediate_rows,
  m.rows AS mart_rows,
  i.rows = m.rows AS reconciled
FROM intermediate_counts AS i
JOIN mart_counts AS m USING (source_type)
ORDER BY source_type;
```

Hasil yang diharapkan: `reconciled = TRUE` untuk sumber `batch` dan `stream`.

## 8. Commit yang Direkomendasikan

### Judul commit

```text
feat: complete NYC Green Taxi batch and streaming pipeline
```

### Deskripsi commit bahasa Indonesia

```text
Menyelesaikan pipeline data NYC Green Taxi secara end-to-end di GCP.

Perubahan:
- Menambahkan DAG Airflow untuk batch loading, transformasi, quality check, dan pembuatan data mart.
- Memperbarui pipeline streaming Apache Beam/Dataflow dan logika transformasi.
- Memperbarui generator event dan publisher Pub/Sub.
- Menambahkan script pembuatan resource GCP dan upload data batch.
- Memperbarui script deployment Dataflow.
- Menambahkan JSON Schema untuk event streaming.
- Menambahkan contoh konfigurasi environment dan dependency proyek.
- Menambahkan README bahasa Indonesia beserta panduan eksekusi end-to-end untuk mentor.
- Menambahkan referensi kode dan laporan validasi bahasa Indonesia.
- Menambahkan bukti eksekusi GCS, Pub/Sub, Dataflow, Airflow, dan BigQuery.
- Memperbarui aturan Git ignore untuk mengecualikan credential, cache, log, dan generated files.
```

Contoh perintah:

```bash
git commit \
  -m "feat: complete NYC Green Taxi batch and streaming pipeline" \
  -m "Add Airflow orchestration, Dataflow streaming transformations, Pub/Sub publisher, GCP setup scripts, validation evidence, and Indonesian mentor documentation."
```

## 9. Penilaian Akhir

Implementasi layak menjadi submission Capstone Project 3 yang kuat karena memperlihatkan pola data engineering batch dan streaming, beberapa lapisan kontrol kualitas data, serta bukti eksekusi cloud yang berhasil. Dengan branch Airflow `taufiq-capstone3` yang sudah dipublikasikan, proyek tidak lagi bergantung pada Docker/Compose yang dianggap hilang; runtime tersebut tersedia secara terpisah sesuai desain dua repositori.

Status akhir: **siap diajukan setelah `.env` dan generated artifacts dipastikan tidak masuk commit, URL repositori Capstone dicantumkan di README, dan hasil pytest terbaru direkam.**
