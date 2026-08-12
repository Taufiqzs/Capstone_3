#!/usr/bin/env bash
# Menyiapkan fondasi GCP yang dapat digunakan kembali untuk Capstone Project 3 Taufiq.

# -e menghentikan script saat terjadi error, -u menolak variabel yang belum diatur,
# dan pipefail menampilkan error yang terjadi di dalam rangkaian pipe.
set -euo pipefail

# GCP_PROJECT_ID menentukan project Google Cloud yang digunakan.
: "${GCP_PROJECT_ID:=jcdeah-009}"
# GCP_REGION menempatkan resource regional di lokasi yang sama untuk mengurangi
# kendala lintas region.
: "${GCP_REGION:=asia-southeast2}"
# STUDENT_ID memberikan identitas kepemilikan yang dapat dilacak pada setiap nama resource.
: "${STUDENT_ID:=taufiqzahrus}"
# GCS_BUCKET menyimpan file mentah dan objek sementara Dataflow.
: "${GCS_BUCKET:=taufiqzahrus-capstone3}"
# BIGQUERY_DATASET mengelompokkan seluruh lapisan data warehouse untuk project ini.
: "${BIGQUERY_DATASET:=taufiqzahrus_capstone3}"
# PUBSUB_TOPIC menerima pesan taksi yang dihasilkan oleh publisher Python.
: "${PUBSUB_TOPIC:=taufiqzahrus-green-taxi-events}"
# PUBSUB_SUBSCRIPTION digunakan oleh Dataflow dan bukan merupakan subscription BigQuery.
: "${PUBSUB_SUBSCRIPTION:=taufiqzahrus-green-taxi-dataflow}"
# RAW_OBJECT_RETENTION_DAYS menghapus data demonstrasi sementara setelah satu bulan.
RAW_OBJECT_RETENTION_DAYS=30
# PUBSUB_ACK_DEADLINE_SECONDS memberikan waktu kepada Dataflow untuk mengonfirmasi
# setiap pengiriman pesan.
PUBSUB_ACK_DEADLINE_SECONDS=60
# PUBSUB_RETENTION_DURATION menyimpan pesan yang belum dikonfirmasi selama tujuh hari.
PUBSUB_RETENTION_DURATION="7d"

# required_variables berisi nilai yang divalidasi sebelum resource cloud diubah.
required_variables=(
  GCP_PROJECT_ID
  GCP_REGION
  STUDENT_ID
  GCS_BUCKET
  BIGQUERY_DATASET
  PUBSUB_TOPIC
  PUBSUB_SUBSCRIPTION
)

# Memvalidasi satu variabel konfigurasi menggunakan ekspansi nama tidak langsung.
# Argumen:
#   $1: Nama environment variable yang akan divalidasi.
# Hasil:
#   Mengembalikan 0 jika nilainya terisi dan bukan placeholder; jika tidak,
#   script akan dihentikan.
require_configuration() {
  # variable_name adalah nama environment variable yang diberikan oleh pemanggil fungsi.
  local variable_name="$1"
  # variable_value membaca nilai target secara tidak langsung dan tetap menangani
  # variabel yang belum diatur.
  local variable_value="${!variable_name:-}"
  if [[ -z "${variable_value}" || "${variable_value}" == your_* ]]; then
    echo "Konfigurasi belum diisi atau masih berupa placeholder: ${variable_name}" >&2
    exit 1
  fi
}

# Memvalidasi seluruh pengenal resource sebelum API atau layanan disiapkan.
for variable_name in "${required_variables[@]}"; do
  require_configuration "${variable_name}"
done

# Mengarahkan perintah gcloud berikutnya ke project yang dituju secara default.
gcloud config set project "${GCP_PROJECT_ID}"

# required_services berisi seluruh API yang digunakan pipeline batch dan streaming.
required_services=(
  bigquery.googleapis.com
  bigquerystorage.googleapis.com
  compute.googleapis.com
  dataflow.googleapis.com
  pubsub.googleapis.com
  storage.googleapis.com
)

# Membaca layanan yang sudah aktif terlebih dahulu. Project yang dikelola penyelenggara
# kursus mungkin mengizinkan pembuatan resource, tetapi membatasi administrasi API
# hanya untuk pemilik project.
mapfile -t enabled_services < <(
  gcloud services list \
    --enabled \
    --project="${GCP_PROJECT_ID}" \
    --format="value(config.name)"
)

# missing_services hanya berisi API yang masih perlu diaktifkan.
missing_services=()
for service_name in "${required_services[@]}"; do
  if ! printf '%s\n' "${enabled_services[@]}" | grep -Fxq "${service_name}"; then
    missing_services+=("${service_name}")
  fi
done

if ((${#missing_services[@]} > 0)); then
  echo "Mengaktifkan API yang belum aktif: ${missing_services[*]}"
  gcloud services enable \
    --project="${GCP_PROJECT_ID}" \
    "${missing_services[@]}"
else
  echo "Semua API Google Cloud yang diperlukan sudah aktif."
fi

# Membuat bucket hanya jika belum tersedia agar script aman dijalankan ulang.
if ! gcloud storage buckets describe "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${GCS_BUCKET}" \
    --location="${GCP_REGION}" \
    --uniform-bucket-level-access
fi

# lifecycle_file menyimpan sementara kebijakan JSON yang dikirim ke gcloud.
lifecycle_file="$(mktemp)"
# Menghapus hanya file lifecycle sementara tersebut ketika script selesai.
trap 'rm -f "${lifecycle_file}"' EXIT
# lifecycle_json mendefinisikan aturan penghapusan otomatis untuk objek di dalam bucket.
lifecycle_json="{\"rule\":[{\"action\":{\"type\":\"Delete\"},\"condition\":{\"age\":${RAW_OBJECT_RETENTION_DAYS}}}]}"
printf '%s\n' "${lifecycle_json}" > "${lifecycle_file}"
gcloud storage buckets update \
  "gs://${GCS_BUCKET}" \
  --lifecycle-file="${lifecycle_file}"

# Membuat dataset BigQuery hanya jika belum tersedia agar aman dijalankan ulang.
if ! bq --project_id="${GCP_PROJECT_ID}" show "${BIGQUERY_DATASET}" >/dev/null 2>&1; then
  bq --project_id="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    mk --dataset "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}"
fi

# Membuat topic streaming hanya jika belum tersedia.
gcloud pubsub topics describe "${PUBSUB_TOPIC}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${PUBSUB_TOPIC}"
# Membuat pull subscription Dataflow hanya jika belum tersedia.
gcloud pubsub subscriptions describe "${PUBSUB_SUBSCRIPTION}" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "${PUBSUB_SUBSCRIPTION}" \
    --topic="${PUBSUB_TOPIC}" \
    --ack-deadline="${PUBSUB_ACK_DEADLINE_SECONDS}" \
    --message-retention-duration="${PUBSUB_RETENTION_DURATION}"

echo "Fondasi GCP berhasil dibuat."