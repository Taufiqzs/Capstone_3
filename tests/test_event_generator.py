"""Unit test untuk pembuatan event taksi deterministik dan serialisasi."""

from datetime import datetime
import json
import random

from publisher.event_generator import EVENT_END, EVENT_START, generate_event


# TEST_SAMPLE_SIZE memberikan cakupan pengujian deterministik yang luas
# tanpa memperlambat pengujian lokal.
TEST_SAMPLE_SIZE = 500

# DOMAIN_TEST_SEED membuat pengujian aturan domain dapat direproduksi
# dengan hasil yang sama persis.
DOMAIN_TEST_SEED = 42

# SERIALIZATION_TEST_SEED membuat contoh serialisasi JSON
# dapat direproduksi.
SERIALIZATION_TEST_SEED = 1


def parse_rfc3339(value: str) -> datetime:
    """Mengubah nilai UTC berformat RFC 3339 menjadi objek datetime.

    Args:
        value: String timestamp yang dihasilkan dan diakhiri dengan karakter Z.

    Returns:
        Objek datetime yang memiliki informasi zona waktu dan dapat digunakan
        untuk memeriksa batas waktu.
    """

    # normalized_value mengubah akhiran Z pada format RFC 3339
    # menjadi offset yang dapat diproses oleh fromisoformat().
    normalized_value = value.replace("Z", "+00:00")

    # parsed_value merupakan objek datetime yang digunakan
    # untuk memeriksa aturan domain pada generator.
    parsed_value = datetime.fromisoformat(normalized_value)

    return parsed_value


def test_generated_events_follow_domain_and_date_rules() -> None:
    """Memastikan banyak event dengan seed mematuhi seluruh aturan bisnis generator."""

    # random_generator membuat seluruh 500 hasil event
    # dapat diulang dengan hasil yang sama.
    random_generator = random.Random(DOMAIN_TEST_SEED)

    for _sample_index in range(TEST_SAMPLE_SIZE):
        # event merupakan satu objek TaxiEvent immutable
        # yang dihasilkan dan sedang diuji.
        event = generate_event(random_generator)

        # pickup_datetime diubah menjadi datetime untuk memeriksa
        # batas waktu inklusif dan eksklusif.
        pickup_datetime = parse_rfc3339(event.pickup_datetime)

        # dropoff_datetime diubah menjadi datetime
        # untuk memvalidasi urutan waktu perjalanan.
        dropoff_datetime = parse_rfc3339(event.dropoff_datetime)

        # Pickup harus berada dalam periode event, drop-off harus terjadi
        # setelah pickup, dan keduanya harus terjadi sebelum EVENT_END.
        assert EVENT_START <= pickup_datetime < dropoff_datetime < EVENT_END

        # ID lokasi pickup harus berada dalam rentang Taxi Zone yang valid.
        assert 1 <= event.pickup_location_id <= 265

        # ID lokasi drop-off harus berada dalam rentang Taxi Zone yang valid.
        assert 1 <= event.dropoff_location_id <= 265

        # Jumlah penumpang harus berada dalam rentang 1 sampai 6.
        assert 1 <= event.passenger_count <= 6

        # Jarak perjalanan harus lebih besar dari nol.
        assert event.trip_distance > 0

        # Total biaya tidak boleh lebih kecil daripada tarif dasar.
        assert event.total_amount >= event.fare_amount

        # Kode metode pembayaran hanya boleh bernilai 1, 2, 3, atau 4.
        assert event.payment_type in {1, 2, 3, 4}

        # Seluruh event yang dibuat oleh generator ini harus berlabel stream.
        assert event.source_type == "stream"


def test_event_is_json_serializable() -> None:
    """Memastikan dictionary event dapat diubah menjadi JSON untuk Pub/Sub."""

    # random_generator menetapkan nilai numerik event yang diserialisasi
    # agar selalu sama setiap kali pengujian dijalankan.
    random_generator = random.Random(SERIALIZATION_TEST_SEED)

    # event_dictionary merupakan struktur payload yang diberikan
    # kepada json.dumps() dalam proses produksi.
    event_dictionary = generate_event(random_generator).to_dict()

    # serialized_event membuktikan bahwa dictionary hanya berisi
    # tipe nilai yang didukung oleh JSON.
    serialized_event = json.dumps(event_dictionary)

    # Objek JSON hasil serialisasi harus diawali dengan kurung kurawal.
    assert serialized_event.startswith("{")