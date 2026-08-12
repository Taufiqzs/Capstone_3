#!/usr/bin/env python3
"""Unduh file Parquet NYC Green Taxi April–Mei 2025 dan unggah ke GCS."""

from __future__ import annotations

import os
import tempfile
import urllib.request
from pathlib import Path

from google.cloud import storage

# SOURCE_BASE_URL adalah direktori resmi NYC TLC yang menyimpan data perjalanan Parquet.
SOURCE_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# SOURCE_MONTHS membatasi proses batch pada April dan Mei 2025 sesuai kebutuhan.
SOURCE_MONTHS = ("2025-04", "2025-05")
# DEFAULT_PROJECT_ID digunakan saat project GCP tidak ditentukan melalui environment.
DEFAULT_PROJECT_ID = "jcdeah-009"
# DEFAULT_BUCKET_NAME adalah tujuan penyimpanan di Cloud Storage.
DEFAULT_BUCKET_NAME = "taufiqzahrus-capstone3"
# GCS_RAW_PREFIX mengelompokkan file sumber asli di dalam satu folder data mentah.
GCS_RAW_PREFIX = "raw"
# DOWNLOAD_TIMEOUT_SECONDS mencegah permintaan yang macet berjalan tanpa batas.
DOWNLOAD_TIMEOUT_SECONDS = 120
# DOWNLOAD_BLOCK_BYTES mengatur ukuran blok pembacaan selama pengunduhan file.
DOWNLOAD_BLOCK_BYTES = 1024 * 1024


def download_atomic(url: str, destination: Path) -> None:
    """Unduh file dan tampilkan hasil akhirnya hanya setelah transfer berhasil.

    Args:
        url: URL HTTPS file Parquet sumber.
        destination: Lokasi akhir file hasil unduhan pada sistem lokal.

    Returns:
        Tidak ada. File yang selesai diunduh ditulis ke ``destination``.

    Raises:
        RuntimeError: Jika sumber mengembalikan file kosong.
        urllib.error.URLError: Jika permintaan ke sumber gagal.
        OSError: Jika file sementara atau file tujuan tidak dapat ditulis.
    """

    # partial_path mencegah file Parquet yang belum selesai dianggap sebagai file utuh.
    partial_path = destination.with_suffix(destination.suffix + ".part")
    # request menyertakan identitas project melalui user agent.
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "taufiqzahrus-capstone3/1.0"},
    )
    # downloaded_bytes menghitung byte unduhan untuk mendeteksi respons kosong.
    downloaded_bytes = 0
    with urllib.request.urlopen(
        request,
        timeout=DOWNLOAD_TIMEOUT_SECONDS,
    ) as response, partial_path.open("wb") as output_file:
        while True:
            # data_block membatasi penggunaan memori saat respons ditulis ke disk.
            data_block = response.read(DOWNLOAD_BLOCK_BYTES)
            if not data_block:
                break
            # output_file menulis blok respons saat ini ke file sementara.
            output_file.write(data_block)
            downloaded_bytes += len(data_block)

    if downloaded_bytes == 0:
        raise RuntimeError(f"File yang diunduh kosong: {url}")
    # Penggantian atomik menampilkan file hanya setelah seluruh byte tersedia.
    partial_path.replace(destination)


def main() -> None:
    """Pastikan setiap file sumber tersedia di bucket GCS yang ditentukan.

    Returns:
        Tidak ada. Objek yang sudah tersedia dilewati dan objek yang belum ada diunggah.

    Raises:
        google.api_core.GoogleAPIError: Jika pemeriksaan atau pengunggahan ke GCS gagal.
        RuntimeError: Jika file sumber yang diunduh kosong.
    """

    # project_id memilih project GCP untuk klien Storage yang terautentikasi.
    project_id = os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID)
    # bucket_name menentukan tujuan data mentah dan dapat diubah melalui environment.
    bucket_name = os.getenv("GCS_BUCKET", DEFAULT_BUCKET_NAME)
    if not bucket_name or bucket_name.startswith("your_"):
        raise SystemExit("Atur GCS_BUCKET dengan nilai yang bukan placeholder")

    # storage_client memakai Application Default Credentials tanpa memuat kunci JSON.
    storage_client = storage.Client(project=project_id)
    # destination_bucket adalah referensi ke bucket tujuan.
    destination_bucket = storage_client.bucket(bucket_name)
    # temporary_directory dihapus otomatis setelah seluruh pengunggahan selesai.
    with tempfile.TemporaryDirectory(prefix="green-taxi-") as temporary_directory:
        for source_month in SOURCE_MONTHS:
            # filename mengikuti konvensi penamaan bulanan resmi NYC TLC.
            filename = f"green_tripdata_{source_month}.parquet"
            # local_path adalah lokasi sementara untuk file bulan ini.
            local_path = Path(temporary_directory) / filename
            # object_name menempatkan file sumber di bawah prefix data mentah.
            object_name = f"{GCS_RAW_PREFIX}/{filename}"
            # destination_blob mewakili objek yang mungkin sudah tersedia di GCS.
            destination_blob = destination_bucket.blob(object_name)
            if destination_blob.exists(storage_client):
                print(f"Sudah tersedia, dilewati: gs://{bucket_name}/{object_name}")
                continue

            # source_url adalah alamat publik resmi file Parquet untuk bulan ini.
            source_url = f"{SOURCE_BASE_URL}/{filename}"
            download_atomic(source_url, local_path)
            destination_blob.upload_from_filename(
                local_path,
                content_type="application/octet-stream",
            )
            print(f"Berhasil diunggah: gs://{bucket_name}/{object_name}")


if __name__ == "__main__":
    # Hindari proses cloud saat modul diimpor oleh test atau alat dokumentasi.
    main()