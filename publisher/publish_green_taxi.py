#!/usr/bin/env python3
"""Mengirim event Green Taxi yang dihasilkan ke Google Cloud Pub/Sub."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from concurrent.futures import Future
from types import FrameType

from event_generator import generate_event


# DEFAULT_PROJECT_ID mengidentifikasi project GCP milik Taufiq
# ketika environment variable tidak menyediakannya.
DEFAULT_PROJECT_ID = "jcdeah-009"

# DEFAULT_TOPIC_NAME mengidentifikasi topic Pub/Sub personal
# yang menjadi tujuan pengiriman pesan.
DEFAULT_TOPIC_NAME = "taufiqzahrus-green-taxi-events"

# DEFAULT_EVENT_RATE adalah jumlah event per detik yang digunakan
# apabila tidak ada konfigurasi lain.
DEFAULT_EVENT_RATE = 5.0

# DEFAULT_EVENT_COUNT adalah jumlah event dalam satu kali proses
# yang digunakan apabila tidak ada konfigurasi lain.
DEFAULT_EVENT_COUNT = 100

# PUBLISH_BATCH_SIZE membatasi jumlah pesan yang dikumpulkan
# Pub/Sub client dalam satu batch.
PUBLISH_BATCH_SIZE = 100

# PUBLISH_BATCH_LATENCY_SECONDS membatasi waktu tunggu batch
# sebelum dikirim oleh Pub/Sub client.
PUBLISH_BATCH_LATENCY_SECONDS = 0.2

# FUTURE_TIMEOUT_SECONDS membatasi waktu tunggu shutdown
# terhadap hasil dari satu proses publish.
FUTURE_TIMEOUT_SECONDS = 60


def parse_args(
    arguments: list[str] | None = None,
) -> argparse.Namespace:
    """Memproses argumen command line dan nilai default dari environment.

    Args:
        arguments: Daftar argumen opsional untuk pengujian. Jika bernilai
            ``None``, fungsi akan membaca argumen dari ``sys.argv``.

    Returns:
        Namespace yang berisi konfigurasi project, topic, rate, count,
        dan dry-run.

    Raises:
        SystemExit: Jika tipe atau sintaksis argumen tidak valid.
    """

    # parser mendefinisikan antarmuka command line publik
    # untuk script publisher.
    parser = argparse.ArgumentParser(description=__doc__)

    # project_default memungkinkan environment variable mengganti
    # nilai project tanpa menghilangkan nilai default personal.
    project_default = os.getenv(
        "GCP_PROJECT_ID",
        DEFAULT_PROJECT_ID,
    )

    # topic_default memungkinkan environment variable mengganti
    # topic tujuan, misalnya dengan topic khusus pengujian.
    topic_default = os.getenv(
        "PUBSUB_TOPIC",
        DEFAULT_TOPIC_NAME,
    )

    # rate_default membaca konfigurasi kecepatan event
    # sebagai bilangan floating-point.
    rate_default = float(
        os.getenv("EVENT_RATE", str(DEFAULT_EVENT_RATE))
    )

    # count_default membaca konfigurasi jumlah event
    # sebagai bilangan bulat.
    count_default = int(
        os.getenv("EVENT_COUNT", str(DEFAULT_EVENT_COUNT))
    )

    parser.add_argument(
        "--project-id",
        default=project_default,
        help="Project GCP yang memiliki topic Pub/Sub.",
    )

    parser.add_argument(
        "--topic",
        default=topic_default,
        help="Nama topic Pub/Sub yang menerima event taksi.",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=rate_default,
        help=(
            "Jumlah event yang dikirim per detik; "
            "nilainya harus lebih besar dari nol."
        ),
    )

    parser.add_argument(
        "--count",
        type=int,
        default=count_default,
        help=(
            "Jumlah event yang akan dikirim; "
            "nilai nol berarti berjalan sampai dihentikan."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mencetak JSON tanpa menghubungi Pub/Sub.",
    )

    # parsed_arguments berisi nilai command line
    # yang sintaksisnya telah divalidasi.
    parsed_arguments = parser.parse_args(arguments)

    return parsed_arguments


def main(arguments: list[str] | None = None) -> int:
    """Menghasilkan event dengan kecepatan terkontrol dan mengirimkannya dengan aman.

    Args:
        arguments: Nilai command line opsional yang diteruskan
            ke fungsi ``parse_args``.

    Returns:
        Kode keluar proses ``0`` setelah seluruh operasi publish
        yang tertunda selesai.

    Raises:
        SystemExit: Jika konfigurasi rate atau count melanggar aturan
            publisher, atau project dan topic GCP tidak tersedia ketika
            program dijalankan tanpa dry-run.
        TimeoutError: Jika Pub/Sub tidak memberikan konfirmasi terhadap
            pesan yang masih tertunda saat proses shutdown.
        Exception: Meneruskan kegagalan publish yang dilaporkan
            oleh Future milik Pub/Sub.
    """

    # args menyimpan konfigurasi runtime efektif yang berasal
    # dari command line dan environment variable.
    args = parse_args(arguments)

    if args.rate <= 0:
        raise SystemExit("--rate harus lebih besar dari nol")

    if args.count < 0:
        raise SystemExit(
            "--count harus bernilai nol (tanpa batas) atau positif"
        )

    if not args.dry_run and (not args.project_id or not args.topic):
        raise SystemExit(
            "GCP_PROJECT_ID dan PUBSUB_TOPIC wajib tersedia, "
            "kecuali jika menggunakan --dry-run"
        )

    # stop_requested berubah menjadi True ketika SIGINT atau SIGTERM
    # meminta program berhenti dengan aman.
    stop_requested = False

    def request_stop(
        _signal_number: int,
        _frame: FrameType | None,
    ) -> None:
        """Mencatat permintaan shutdown tanpa mengabaikan pesan yang tertunda.

        Args:
            _signal_number: Nomor sinyal sistem operasi. Nilai ini tidak
                digunakan setelah sinyal diterima.
            _frame: Frame interpreter saat ini yang diberikan
                oleh signal API milik Python.

        Returns:
            Tidak mengembalikan nilai. Flag ``stop_requested`` dari
            enclosing scope diperbarui secara langsung.
        """

        nonlocal stop_requested

        # Pengaturan flag membuat loop utama berhenti
        # pada batas iterasi yang aman.
        stop_requested = True

    # Mendaftarkan satu graceful handler untuk penghentian interaktif
    # maupun penghentian oleh service manager.
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    # publisher tetap bernilai None dalam mode dry-run agar
    # cloud client tidak dibuat.
    publisher = None

    # topic_path menyimpan nama lengkap resource Pub/Sub
    # hanya ketika publisher benar-benar mengirim pesan.
    topic_path = None

    # publish_futures menyimpan acknowledgement yang harus
    # diselesaikan sebelum proses shutdown.
    publish_futures: list[Future] = []

    if not args.dry_run:
        # Import dilakukan hanya ketika dibutuhkan agar dry-run lokal
        # dapat berjalan sebelum package Google Cloud diinstal.
        from google.cloud import pubsub_v1

        # batch_settings mengatur perilaku batching jaringan
        # pada Pub/Sub client.
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=PUBLISH_BATCH_SIZE,
            max_latency=PUBLISH_BATCH_LATENCY_SECONDS,
        )

        # publisher mengirim payload bytes secara asynchronous
        # menggunakan kebijakan batch di atas.
        publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings
        )

        # topic_path adalah nama resource yang siap digunakan API
        # untuk topic tujuan yang telah dikonfigurasi.
        topic_path = publisher.topic_path(
            args.project_id,
            args.topic,
        )

    # publish_interval_seconds adalah target jeda waktu
    # antara dua event yang dikirim secara berurutan.
    publish_interval_seconds = 1.0 / args.rate

    # sent_count menghitung jumlah event yang telah dicetak
    # atau berhasil dimasukkan ke antrean selama proses berjalan.
    sent_count = 0

    try:
        while (
            not stop_requested
            and (args.count == 0 or sent_count < args.count)
        ):
            # iteration_started mencatat waktu mulai pemrosesan agar
            # kecepatan pengiriman tetap stabil sesuai target.
            iteration_started = time.monotonic()

            # event_dictionary adalah event bisnis hasil generator
            # yang telah siap untuk di-encode.
            event_dictionary = generate_event().to_dict()

            # payload mengubah event menjadi JSON ringkas dalam bentuk
            # UTF-8 bytes sesuai persyaratan Pub/Sub.
            payload = json.dumps(
                event_dictionary,
                separators=(",", ":"),
            ).encode("utf-8")

            if args.dry_run:
                # Output dry-run mendukung pemeriksaan offline
                # dan pengujian otomatis.
                print(
                    payload.decode("utf-8"),
                    flush=True,
                )
            else:
                # Kedua nilai berikut dijamin tersedia karena program
                # telah melewati inisialisasi non-dry-run.
                assert publisher is not None
                assert topic_path is not None

                # publish_future nantinya mengonfirmasi keberhasilan
                # atau menampilkan error dari Pub/Sub API.
                publish_future = publisher.publish(
                    topic_path,
                    payload,
                    event_id=str(event_dictionary["event_id"]),
                    source_type="stream",
                )

                publish_futures.append(publish_future)

            # sent_count bertambah hanya setelah event berhasil
            # dicetak atau dimasukkan ke antrean.
            sent_count += 1

            # elapsed_seconds mencatat waktu pemrosesan
            # yang digunakan oleh iterasi saat ini.
            elapsed_seconds = (
                time.monotonic() - iteration_started
            )

            # remaining_delay_seconds mempertahankan rate yang diminta
            # dan mencegah durasi sleep menjadi negatif.
            remaining_delay_seconds = max(
                0.0,
                publish_interval_seconds - elapsed_seconds,
            )

            time.sleep(remaining_delay_seconds)

    finally:
        # Menyelesaikan setiap Future yang masuk antrean agar pesan
        # terkirim dan error publish dapat ditampilkan.
        for publish_future in publish_futures:
            publish_future.result(
                timeout=FUTURE_TIMEOUT_SECONDS
            )

    print(
        f"Publisher berhenti dengan aman. Event terkirim: {sent_count}",
        file=sys.stderr,
    )

    return 0


if __name__ == "__main__":
    # Mengubah return code dari main menjadi exit status
    # untuk sistem operasi.
    raise SystemExit(main())