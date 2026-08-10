"""Unit test untuk fungsi validasi Dataflow yang tidak bergantung pada cloud."""

import json
import random

from dataflow.transforms import validate_and_transform
from publisher.event_generator import generate_event


# VALID_EVENT_SEED membuat event deterministik
# yang digunakan dalam pemeriksaan hasil enrichment.
VALID_EVENT_SEED = 7

# INVALID_DATE_SEED membuat event dasar yang kemudian
# diubah menggunakan timestamp bulan Agustus.
INVALID_DATE_SEED = 8

# INVALID_AUGUST_TIMESTAMP sengaja melanggar aturan
# bahwa waktu pickup harus berada pada periode Juni–Juli.
INVALID_AUGUST_TIMESTAMP = "2025-08-01T00:00:00Z"

# BROKEN_JSON_PAYLOAD sengaja dibuat agar tidak dapat diproses sebagai JSON.
BROKEN_JSON_PAYLOAD = b"not-json"


def test_valid_event_is_enriched() -> None:
    """Memastikan payload yang valid menghasilkan output yang telah diperkaya."""

    # random_generator menetapkan setiap nilai input simulasi
    # agar hasil pengujian dapat diulang.
    random_generator = random.Random(VALID_EVENT_SEED)

    # event_dictionary berisi field sumber yang dihasilkan
    # sebelum diproses oleh logika Dataflow.
    event_dictionary = generate_event(random_generator).to_dict()

    # payload memiliki bentuk byte JSON UTF-8
    # seperti data yang diterima dari Pub/Sub.
    payload = json.dumps(event_dictionary).encode("utf-8")

    # is_valid menunjukkan keputusan routing, sedangkan result_row
    # berisi output yang siap dimasukkan ke BigQuery.
    is_valid, result_row = validate_and_transform(payload)

    # Event harus dinyatakan valid.
    assert is_valid

    # pickup_date harus berasal dari tahun 2025.
    assert result_row["pickup_date"].startswith("2025-0")

    # Durasi perjalanan hasil enrichment harus lebih besar dari nol.
    assert result_row["trip_duration_minutes"] > 0

    # Tarif per mil hasil enrichment harus lebih besar dari nol.
    assert result_row["fare_per_mile"] > 0


def test_invalid_date_goes_to_dead_letter() -> None:
    """Memastikan pickup bulan Agustus ditolak dengan alasan yang jelas."""

    # random_generator membuat event dasar yang valid
    # sebelum dilakukan satu perubahan yang terkontrol.
    random_generator = random.Random(INVALID_DATE_SEED)

    # event_dictionary kemudian diubah agar mengandung
    # pelanggaran batas tanggal yang disengaja.
    event_dictionary = generate_event(random_generator).to_dict()
    event_dictionary["event_time"] = INVALID_AUGUST_TIMESTAMP
    event_dictionary["pickup_datetime"] = INVALID_AUGUST_TIMESTAMP

    # payload merupakan teks JSON tidak valid secara aturan bisnis
    # yang dikirimkan ke fungsi validasi.
    payload = json.dumps(event_dictionary)

    # is_valid dan result_row menunjukkan keputusan dead-letter
    # beserta penjelasan penyebab penolakannya.
    is_valid, result_row = validate_and_transform(payload)

    # Event harus dinyatakan tidak valid.
    assert not is_valid

    # Alasan penolakan harus menjelaskan bahwa tanggal
    # berada di luar periode Juni–Juli.
    assert "outside June-July" in result_row["error_reason"]


def test_broken_json_goes_to_dead_letter() -> None:
    """Memastikan JSON rusak dipertahankan tanpa menghentikan pipeline."""

    # is_valid dan result_row menangkap penolakan aman
    # terhadap payload byte dengan format JSON yang rusak.
    is_valid, result_row = validate_and_transform(BROKEN_JSON_PAYLOAD)

    # Payload harus dinyatakan tidak valid.
    assert not is_valid

    # Payload mentah harus tetap disimpan untuk pemeriksaan.
    assert result_row["raw_payload"] == "not-json"


def test_non_object_json_goes_to_dead_letter() -> None:
    """Memastikan JSON dengan tipe struktur yang salah ditolak secara aman."""

    # array_payload merupakan JSON yang valid secara sintaks,
    # tetapi melanggar kontrak karena bukan sebuah object.
    array_payload = "[]"

    # is_valid dan result_row menangkap hasil validasi struktur payload.
    is_valid, result_row = validate_and_transform(array_payload)

    # Payload harus dinyatakan tidak valid.
    assert not is_valid

    # Alasan penolakan harus menjelaskan bahwa payload wajib berupa object.
    assert "must be an object" in result_row["error_reason"]