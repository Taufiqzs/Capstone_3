"""Menyediakan validasi dan transformasi event taksi yang tidak bergantung pada cloud."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


# EVENT_START adalah batas bawah inklusif untuk timestamp pickup simulasi.
EVENT_START = datetime(2025, 6, 1, tzinfo=timezone.utc)

# EVENT_END adalah batas atas eksklusif untuk timestamp pickup dan drop-off.
EVENT_END = datetime(2025, 8, 1, tzinfo=timezone.utc)

# MIN_LOCATION_ID adalah kode NYC Taxi Zone terkecil yang diterima.
MIN_LOCATION_ID = 1

# MAX_LOCATION_ID adalah kode NYC Taxi Zone terbesar
# yang diterima dalam proyek ini.
MAX_LOCATION_ID = 265

# MIN_PASSENGER_COUNT mencegah event perjalanan tanpa penumpang.
MIN_PASSENGER_COUNT = 1

# MAX_PASSENGER_COUNT membatasi event simulasi
# berdasarkan kapasitas standar taksi.
MAX_PASSENGER_COUNT = 6

# VALID_PAYMENT_TYPES berisi seluruh kode kategori pembayaran yang diterima.
VALID_PAYMENT_TYPES = {1, 2, 3, 4}

# MONETARY_COMPONENT_FIELDS berisi field nilai uang
# yang boleh bernilai nol, tetapi tidak boleh negatif.
MONETARY_COMPONENT_FIELDS = (
    "fare_amount",
    "tip_amount",
    "tolls_amount",
)

# REQUIRED_FIELDS mendefinisikan kontrak minimum JSON
# sebelum validasi aturan domain dilakukan.
REQUIRED_FIELDS = {
    "event_id",
    "event_time",
    "ingestion_time",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "payment_type",
    "source_type",
}


def parse_timestamp(value: object) -> datetime:
    """Memproses timestamp RFC 3339 dan menormalisasikannya ke UTC.

    Args:
        value: Nilai timestamp yang akan diperiksa dari event
            yang telah melalui proses decoding.

    Returns:
        Objek ``datetime`` yang memiliki informasi zona waktu
        dan telah dinormalisasi ke UTC.

    Raises:
        ValueError: Jika nilai bukan string atau bukan timestamp ISO
            yang valid.
    """

    if not isinstance(value, str):
        raise ValueError("timestamp harus berupa string")

    # normalized_value memungkinkan Python memproses
    # akhiran UTC standar dalam format RFC 3339.
    normalized_value = value.replace("Z", "+00:00")

    # parsed_value mempertahankan informasi zona waktu
    # agar keberadaan timezone dapat diperiksa secara eksplisit.
    parsed_value = datetime.fromisoformat(normalized_value)

    if parsed_value.tzinfo is None:
        raise ValueError("timestamp harus menyertakan offset zona waktu")

    # utc_timestamp telah dinormalisasi ke UTC sehingga perbandingan
    # dengan batas waktu dapat dilakukan secara konsisten.
    utc_timestamp = parsed_value.astimezone(timezone.utc)

    return utc_timestamp


def validate_and_transform(
    payload: bytes | str,
) -> tuple[bool, dict[str, Any]]:
    """Memvalidasi satu payload Pub/Sub dan memperkaya event valid untuk BigQuery.

    Args:
        payload: JSON UTF-8 yang diberikan sebagai bytes oleh Pub/Sub
            atau sebagai teks dalam proses pengujian.

    Returns:
        Mengembalikan ``(True, valid_row)`` ketika seluruh aturan validasi
        berhasil dipenuhi. Jika validasi gagal, fungsi mengembalikan
        ``(False, dead_letter_row)`` yang berisi payload asli dan
        alasan kegagalannya.

    Notes:
        Error validasi diubah menjadi dead-letter row dan tidak diteruskan
        sebagai exception. Dengan demikian, streaming pipeline dapat terus
        memproses event berikutnya meskipun menemukan event tidak valid.
    """

    # raw_payload adalah bentuk teks dari payload yang dipertahankan
    # untuk parsing JSON atau keperluan diagnosis error.
    raw_payload = (
        payload.decode("utf-8", errors="replace")
        if isinstance(payload, bytes)
        else payload
    )

    try:
        # event adalah objek JSON hasil decoding
        # yang seluruh field-nya akan divalidasi.
        event = json.loads(raw_payload)

        if not isinstance(event, dict):
            raise ValueError("payload JSON harus berupa object")

        # missing_fields berisi daftar key wajib
        # yang tidak ditemukan dalam event ini.
        missing_fields = sorted(REQUIRED_FIELDS - event.keys())

        if missing_fields:
            raise ValueError(
                f"field yang tidak tersedia: {', '.join(missing_fields)}"
            )

        # pickup_datetime digunakan untuk memvalidasi bahwa waktu pickup
        # berada dalam periode Juni–Juli 2025.
        pickup_datetime = parse_timestamp(
            event["pickup_datetime"]
        )

        # dropoff_datetime harus terjadi setelah pickup
        # dan tetap berada sebelum Agustus 2025.
        dropoff_datetime = parse_timestamp(
            event["dropoff_datetime"]
        )

        # event_time diproses secara eksplisit untuk menolak
        # timestamp event bisnis yang tidak valid.
        event_time = parse_timestamp(
            event["event_time"]
        )

        # ingestion_time diproses secara eksplisit untuk mendukung
        # pemeriksaan kesegaran data pada tahap berikutnya.
        ingestion_time = parse_timestamp(
            event["ingestion_time"]
        )

        if event_time != pickup_datetime:
            raise ValueError(
                "event_time harus sama dengan pickup_datetime"
            )

        if ingestion_time > datetime.now(timezone.utc):
            raise ValueError(
                "ingestion_time tidak boleh berada di masa depan"
            )

        if not EVENT_START <= pickup_datetime < EVENT_END:
            raise ValueError(
                "pickup_datetime berada di luar periode Juni–Juli 2025"
            )

        if not pickup_datetime < dropoff_datetime < EVENT_END:
            raise ValueError(
                "dropoff_datetime harus terjadi setelah pickup "
                "dan tetap berada sebelum Agustus"
            )

        # pickup_location_id mengubah field yang diterima menjadi integer
        # agar rentang nilainya dapat divalidasi.
        pickup_location_id = int(
            event["pickup_location_id"]
        )

        # dropoff_location_id mengubah field yang diterima menjadi integer
        # agar rentang nilainya dapat divalidasi.
        dropoff_location_id = int(
            event["dropoff_location_id"]
        )

        # passenger_count mengubah field yang diterima menjadi integer
        # agar kapasitas penumpang dapat divalidasi.
        passenger_count = int(
            event["passenger_count"]
        )

        # payment_type mengubah kategori pembayaran yang diterima
        # menjadi kode integer.
        payment_type = int(
            event["payment_type"]
        )

        # trip_distance mengubah jarak perjalanan yang diterima
        # menjadi nilai numerik.
        trip_distance = float(
            event["trip_distance"]
        )

        # total_amount mengubah keseluruhan biaya perjalanan
        # menjadi nilai numerik.
        total_amount = float(
            event["total_amount"]
        )

        # monetary_components berisi nilai tarif, tip, dan tol
        # yang tidak boleh bernilai negatif.
        monetary_components = [
            float(event[field_name])
            for field_name in MONETARY_COMPONENT_FIELDS
        ]

        if not (
            MIN_LOCATION_ID
            <= pickup_location_id
            <= MAX_LOCATION_ID
        ):
            raise ValueError(
                "pickup_location_id tidak valid"
            )

        if not (
            MIN_LOCATION_ID
            <= dropoff_location_id
            <= MAX_LOCATION_ID
        ):
            raise ValueError(
                "dropoff_location_id tidak valid"
            )

        if not (
            MIN_PASSENGER_COUNT
            <= passenger_count
            <= MAX_PASSENGER_COUNT
        ):
            raise ValueError(
                "passenger_count tidak valid"
            )

        if payment_type not in VALID_PAYMENT_TYPES:
            raise ValueError(
                "payment_type tidak valid"
            )

        if trip_distance <= 0:
            raise ValueError(
                "trip_distance harus bernilai positif"
            )

        if min(monetary_components) < 0:
            raise ValueError(
                "komponen biaya tidak boleh bernilai negatif"
            )

        if total_amount <= 0:
            raise ValueError(
                "total_amount harus bernilai positif"
            )

        if event["source_type"] != "stream":
            raise ValueError(
                "source_type harus bernilai stream"
            )

        # trip_duration_minutes dihitung pada tahap transformasi
        # agar logika yang sama tidak perlu dihitung ulang menggunakan SQL.
        trip_duration_minutes = round(
            (
                dropoff_datetime - pickup_datetime
            ).total_seconds()
            / 60,
            2,
        )

        # fare_per_mile mengukur nilai tarif dasar
        # untuk setiap mil perjalanan.
        fare_per_mile = round(
            float(event["fare_amount"]) / trip_distance,
            2,
        )

        # processing_time mencatat waktu ketika Dataflow
        # menyelesaikan proses transformasi event ini.
        processing_time = datetime.now(
            timezone.utc
        ).isoformat()

        # transformed_row menyalin seluruh field dari event sumber
        # sebelum menambahkan atribut turunan.
        transformed_row = dict(event)

        transformed_row.update(
            pickup_date=pickup_datetime.date().isoformat(),
            trip_duration_minutes=trip_duration_minutes,
            fare_per_mile=fare_per_mile,
            processing_time=processing_time,
        )

        return True, transformed_row

    except (
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        # dead_letter_row mempertahankan bukti kegagalan
        # tanpa menghentikan streaming job.
        dead_letter_row = {
            "raw_payload": raw_payload,
            "error_reason": str(error),
            "processing_time": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        return False, dead_letter_row