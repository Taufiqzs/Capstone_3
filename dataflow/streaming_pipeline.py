#!/usr/bin/env python3
"""Menjalankan pipeline ETL streaming dari Pub/Sub ke BigQuery menggunakan Dataflow."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from dataflow.transforms import validate_and_transform


# VALID_SCHEMA memetakan setiap field event hasil transformasi
# ke tipe data yang sesuai di BigQuery.
VALID_SCHEMA = {
    "fields": [
        {
            "name": "event_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "event_time",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "ingestion_time",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "pickup_datetime",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "dropoff_datetime",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "pickup_date",
            "type": "DATE",
            "mode": "REQUIRED",
        },
        {
            "name": "pickup_location_id",
            "type": "INTEGER",
            "mode": "REQUIRED",
        },
        {
            "name": "dropoff_location_id",
            "type": "INTEGER",
            "mode": "REQUIRED",
        },
        {
            "name": "passenger_count",
            "type": "INTEGER",
            "mode": "REQUIRED",
        },
        {
            "name": "trip_distance",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "trip_duration_minutes",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "fare_amount",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "tip_amount",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "tolls_amount",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "total_amount",
            "type": "FLOAT",
            "mode": "REQUIRED",
        },
        {
            "name": "fare_per_mile",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "payment_type",
            "type": "INTEGER",
            "mode": "REQUIRED",
        },
        {
            "name": "source_type",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "processing_time",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
    ]
}


# DEAD_LETTER_SCHEMA menentukan struktur penyimpanan payload tidak valid,
# lengkap dengan alasan kegagalan dan waktu pemrosesan.
DEAD_LETTER_SCHEMA = {
    "fields": [
        {
            "name": "raw_payload",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "error_reason",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "processing_time",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
    ]
}


# VALID_OUTPUT_TAG adalah nama side output Beam
# yang berisi event valid dan siap disimpan ke data warehouse.
VALID_OUTPUT_TAG = "valid"

# INVALID_OUTPUT_TAG adalah nama side output Beam
# yang berisi event yang ditolak karena tidak valid.
INVALID_OUTPUT_TAG = "invalid"


class ValidateAndTransform(beam.DoFn):
    """Mengarahkan satu payload Pub/Sub ke side output valid atau tidak valid.

    Attributes:
        VALID: Tag output untuk baris yang telah diperkaya dan berhasil
            memenuhi seluruh pemeriksaan validasi.
        INVALID: Tag output untuk baris dead-letter yang berisi informasi
            diagnosis kegagalan.
    """

    # VALID disediakan pada tingkat class agar pembuatan pipeline
    # selalu menggunakan nama tag yang konsisten.
    VALID = VALID_OUTPUT_TAG

    # INVALID disediakan pada tingkat class untuk memastikan
    # nama tag tidak ditulis berbeda di bagian lain.
    INVALID = INVALID_OUTPUT_TAG

    def process(self, payload: bytes) -> Iterator[Any]:
        """Memvalidasi, mentransformasi, dan memberikan tag pada pesan Pub/Sub.

        Args:
            payload: Isi mentah pesan Pub/Sub yang diberikan oleh
                ``ReadFromPubSub``.

        Yields:
            Satu objek ``TaggedOutput`` yang berisi event hasil transformasi
            atau baris dead-letter. Setiap payload input selalu menghasilkan
            tepat satu output.
        """

        # is_valid menunjukkan side output yang harus menerima baris hasil.
        is_valid, result_row = validate_and_transform(payload)

        # output_tag memilih jalur valid atau tidak valid
        # tanpa menghilangkan data yang sedang diproses.
        output_tag = self.VALID if is_valid else self.INVALID

        yield beam.pvalue.TaggedOutput(
            output_tag,
            result_row,
        )


def build_pipeline(
    pipeline: beam.Pipeline,
    input_subscription: str,
    output_table: str,
    dead_letter_table: str,
) -> None:
    """Menambahkan input Pub/Sub, validasi, dan output BigQuery ke pipeline Beam.

    Args:
        pipeline: Pipeline Apache Beam aktif yang akan menerima rangkaian
            transformasi.
        input_subscription: Path lengkap resource pull subscription Pub/Sub.
        output_table: Spesifikasi tabel BigQuery untuk menyimpan event
            yang diterima.
        dead_letter_table: Spesifikasi tabel BigQuery untuk menyimpan event
            yang ditolak.

    Returns:
        Tidak mengembalikan nilai. Fungsi ini mengubah graph pipeline
        yang diberikan secara langsung.
    """

    # routed_results menyediakan PCollection valid dan tidak valid
    # secara terpisah berdasarkan tag output masing-masing.
    routed_results = (
        pipeline
        | "ReadPubSub"
        >> beam.io.ReadFromPubSub(
            subscription=input_subscription
        )
        | "ValidateTransform"
        >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs(
            ValidateAndTransform.VALID,
            ValidateAndTransform.INVALID,
        )
    )

    # Menulis event valid menggunakan Storage Write API
    # untuk mendukung proses ingestion dengan latensi rendah.
    routed_results.valid | "WriteValidBigQuery" >> beam.io.WriteToBigQuery(
        output_table,
        schema=VALID_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        additional_bq_parameters={
            # Melakukan partisi berdasarkan tanggal bisnis agar query
            # yang menggunakan filter tanggal membaca lebih sedikit data.
            "timePartitioning": {
                "type": "DAY",
                "field": "pickup_date",
            },
            # Melakukan clustering berdasarkan dimensi lokasi pickup
            # dan metode pembayaran yang sering digunakan dalam analisis.
            "clustering": {
                "fields": [
                    "pickup_location_id",
                    "payment_type",
                ]
            },
        },
    )

    # Menulis event yang ditolak ke tabel terpisah agar masalah kualitas
    # data tetap dapat dipantau, diperiksa, dan diaudit.
    routed_results.invalid | "WriteInvalidBigQuery" >> (
        beam.io.WriteToBigQuery(
            dead_letter_table,
            schema=DEAD_LETTER_SCHEMA,
            create_disposition=(
                beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            ),
            write_disposition=(
                beam.io.BigQueryDisposition.WRITE_APPEND
            ),
        )
    )


def main(arguments: list[str] | None = None) -> None:
    """Memproses parameter khusus dan menjalankan pipeline streaming.

    Args:
        arguments: Daftar argumen command line opsional. Jika bernilai
            ``None``, argumen akan dibaca dari ``sys.argv``.

    Returns:
        Tidak mengembalikan nilai. Context Beam akan mengirimkan job
        dan mengelola penyelesaian proses runner.

    Raises:
        SystemExit: Jika salah satu argumen khusus pipeline yang wajib
            tidak diberikan.
    """

    # argument_parser hanya menangani argumen khusus proyek,
    # sedangkan argumen lainnya akan diproses oleh Apache Beam.
    argument_parser = argparse.ArgumentParser(
        description=__doc__
    )

    argument_parser.add_argument(
        "--input-subscription",
        required=True,
        help=(
            "Path lengkap resource pull subscription Pub/Sub."
        ),
    )

    argument_parser.add_argument(
        "--output-table",
        required=True,
        help=(
            "Tabel tujuan BigQuery dalam format "
            "PROJECT:DATASET.TABLE."
        ),
    )

    argument_parser.add_argument(
        "--dead-letter-table",
        required=True,
        help=(
            "Tabel tujuan BigQuery untuk menyimpan payload "
            "yang ditolak."
        ),
    )

    # known_arguments berisi tiga parameter khusus
    # yang digunakan oleh modul ini.
    known_arguments, pipeline_arguments = (
        argument_parser.parse_known_args(arguments)
    )

    # pipeline_options meneruskan konfigurasi runner, project,
    # region, dan pengaturan lainnya ke Apache Beam.
    pipeline_options = PipelineOptions(
        pipeline_arguments,
        save_main_session=True,
        streaming=True,
    )

    # standard_options secara eksplisit mengaktifkan mode streaming
    # untuk memproses sumber data yang tidak memiliki batas akhir.
    standard_options = pipeline_options.view_as(
        StandardOptions
    )
    standard_options.streaming = True

    # setup_options menyertakan definisi dari main session
    # agar tersedia pada worker Dataflow yang berjalan secara remote.
    setup_options = pipeline_options.view_as(
        SetupOptions
    )
    setup_options.save_main_session = True

    # pipeline_context mengirimkan graph pipeline dan mengelola
    # siklus hidup runner secara otomatis.
    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline_context:
        build_pipeline(
            pipeline_context,
            known_arguments.input_subscription,
            known_arguments.output_table,
            known_arguments.dead_letter_table,
        )


if __name__ == "__main__":
    # Menjalankan fungsi main hanya ketika file dieksekusi sebagai script
    # atau modul Python, bukan ketika file diimpor.
    main()