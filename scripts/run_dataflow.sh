#!/usr/bin/env bash

# Mengirim pipeline streaming Pub/Sub-ke-BigQuery milik Taufiq
# ke Google Cloud Dataflow.

# -e menghentikan script ketika terjadi error.
# -u menolak penggunaan variabel yang belum didefinisikan.
# pipefail memastikan error dalam rangkaian pipe terdeteksi.
set -euo pipefail


# GCP_PROJECT_ID menentukan Google Cloud project yang digunakan.
: "${GCP_PROJECT_ID:=jcdeah-009}"

# GCP_REGION menentukan region Dataflow dan lokasi resource terkait.
: "${GCP_REGION:=us-central1}"

# STUDENT_ID digunakan sebagai awalan nama job agar pemilik job
# dapat dikenali melalui Google Cloud Console.
: "${STUDENT_ID:=taufiqzahrus}"

# GCS_BUCKET menyimpan file sementara dan artifact staging Dataflow.
: "${GCS_BUCKET:=taufiqzahrus-capstone3}"

# BIGQUERY_DATASET menampung tabel streaming valid dan dead-letter.
: "${BIGQUERY_DATASET:=taufiqzahrus_capstone3}"

# PUBSUB_SUBSCRIPTION adalah pull subscription khusus
# yang digunakan oleh job Dataflow ini.
: "${PUBSUB_SUBSCRIPTION:=taufiqzahrus-green-taxi-dataflow}"


# required_variables berisi seluruh variabel konfigurasi
# yang wajib memiliki nilai sebelum job dikirim.
required_variables=(
    GCP_PROJECT_ID
    GCP_REGION
    STUDENT_ID
    GCS_BUCKET
    BIGQUERY_DATASET
    PUBSUB_SUBSCRIPTION
)


# Memvalidasi satu variabel konfigurasi menggunakan indirect expansion.
#
# Arguments:
#   $1: Nama environment variable yang akan divalidasi.
#
# Returns:
#   Mengembalikan status 0 apabila variabel memiliki nilai dan bukan placeholder.
#   Script akan dihentikan apabila konfigurasi tidak valid.
require_configuration() {
    # variable_name adalah nama environment variable
    # yang diberikan oleh pemanggil fungsi.
    local variable_name="$1"

    # variable_value membaca nilai variabel secara tidak langsung
    # dan tetap menangani variabel yang belum didefinisikan.
    local variable_value="${!variable_name:-}"

    if [[ -z "${variable_value}" || "${variable_value}" == your_* ]]; then
        echo "Konfigurasi belum diisi atau masih berupa placeholder: ${variable_name}" >&2
        exit 1
    fi
}


# Memvalidasi seluruh konfigurasi wajib sebelum menjalankan
# cloud job yang dapat menimbulkan biaya.
for variable_name in "${required_variables[@]}"; do
    require_configuration "${variable_name}"
done


# normalized_student_id mengubah underscore menjadi tanda hubung
# agar sesuai dengan aturan penamaan job Dataflow.
normalized_student_id="${STUDENT_ID//_/-}"

# job_timestamp membuat nama setiap job unik sekaligus mencatat
# waktu peluncurannya dalam zona waktu UTC.
job_timestamp="$(date -u +%Y%m%d-%H%M%S)"

# job_name adalah identifier yang ditampilkan untuk job streaming
# ini pada Dataflow.
job_name="${normalized_student_id}-green-taxi-stream-${job_timestamp}"

# input_subscription_path adalah path lengkap resource Pub/Sub
# subscription yang akan dibaca oleh Apache Beam.
input_subscription_path="projects/${GCP_PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION}"

# valid_output_table menyimpan seluruh event taksi
# yang valid dan telah diperkaya.
valid_output_table="${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.stg_green_taxi_stream"

# dead_letter_table menyimpan payload yang ditolak
# beserta alasan kegagalan validasinya.
dead_letter_table="${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.stg_green_taxi_stream_dead_letter"


# Mengirim pipeline tanpa batas akhir ke Dataflow.
# Hentikan job setelah bukti pelaksanaan proyek berhasil dikumpulkan.
python -m dataflow.streaming_pipeline \
    --runner=DataflowRunner \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --job_name="${job_name}" \
    --temp_location="gs://${GCS_BUCKET}/dataflow/temp" \
    --staging_location="gs://${GCS_BUCKET}/dataflow/staging" \
    --input-subscription="${input_subscription_path}" \
    --output-table="${valid_output_table}" \
    --dead-letter-table="${dead_letter_table}" \
    --requirements_file=requirements.txt \
    --max_num_workers=2 \
    --autoscaling_algorithm=THROUGHPUT_BASED