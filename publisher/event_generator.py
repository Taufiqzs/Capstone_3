"""
Menghasilkan data perjalanan Green Taxi buatan yang realistis, konsisten, 
dan memiliki tanggal perjalanan pada Juni–Juli 2025.


Konstanta-konstanta dalam modul ini membuat tujuan bisnis dari setiap nilai yang dihasilkan terlihat jelas. 
Tidak ada informasi autentikasi atau data rahasia yang disimpan dalam file kode ini.

    
"""

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

# EVENT_START adalah batas bawah timestamp yang bersifat inklusif
EVENT_START = datetime(2025, 6, 1, tzinfo=timezone.utc)

# EVENT_END adalah batas atas eksklusif yang mencegah event
# masuk ke periode Agustus.
EVENT_END = datetime(2025, 8, 1, tzinfo=timezone.utc)

# MIN_TRIP_MINUTES mencegah perjalanan simulasi berdurasi nol
# atau memiliki durasi yang terlalu singkat dan tidak realistis.
MIN_TRIP_MINUTES = 3

# MAX_TRIP_MINUTES membatasi durasi perjalanan dan menyediakan
# rentang waktu agar drop-off tetap terjadi sebelum EVENT_END.
MAX_TRIP_MINUTES = 90

# MIN_LOCATION_ID adalah ID pertama yang valid
# dalam daftar zona taksi New York City.
MIN_LOCATION_ID = 1

# MAX_LOCATION_ID adalah ID terakhir zona taksi New York City
# yang digunakan dalam proyek ini.
MAX_LOCATION_ID = 265

# MIN_DISTANCE_MILES mencegah terbentuknya event perjalanan
# dengan jarak nol mil.
MIN_DISTANCE_MILES = 0.2

# MAX_DISTANCE_MILES membatasi nilai jarak ekstrem
# pada perjalanan perkotaan yang dihasilkan.
MAX_DISTANCE_MILES = 45.0

# BASE_FARE_DOLLARS adalah tarif dasar simulasi yang ditambahkan
# sebelum menghitung biaya berdasarkan jarak tempuh.
BASE_FARE_DOLLARS = 3.0

# SURCHARGE_DOLLARS merepresentasikan pajak atau biaya tambahan tetap
# yang telah disederhanakan untuk keperluan simulasi.
SURCHARGE_DOLLARS = 1.0

# TOLL_CHARGE_DOLLARS adalah nilai biaya tol simulasi
# yang sesekali dikenakan pada perjalanan.
TOLL_CHARGE_DOLLARS = 6.94

# PAYMENT_TYPES berisi kode kategori metode pembayaran
# bergaya NYC yang diterima oleh generator.
PAYMENT_TYPES = (1, 2, 3, 4)

# PAYMENT_WEIGHTS membuat pembayaran dengan kartu dan uang tunai
# lebih sering muncul daripada kategori pembayaran lainnya.
PAYMENT_WEIGHTS = (68, 28, 2, 2)

# PASSENGER_COUNTS berisi semua jumlah penumpang valid
# yang dapat dihasilkan oleh generator.
PASSENGER_COUNTS = (1, 2, 3, 4, 5, 6)

# PASSENGER_WEIGHTS membuat perjalanan dengan satu dan dua penumpang
# menjadi hasil yang paling sering dihasilkan.
PASSENGER_WEIGHTS = (70, 18, 5, 3, 2, 2)


@dataclass(frozen=True)
class TaxiEvent:
    """representasi satu immutable taxi event sebelum di serialized ke JSON.
  
    Attributes/atribut:
        event_id: Globally unique/Unik UUID digunakan untuk mengindentifikasi dan menghapus duplikasi (downstream deduplication).
        event_time: waktu terjadinya perjalanan simulasi. Business-event timestamp, equal to the pickup time.
        ingestion_time: waktu aktual saat python menghasilkan event. Actual UTC time when the generator created the event. 
        pickup_datetime: waktu penumpang dijemput. Simulated passenger pickup timestamp.
        dropoff_datetime: waktu penumpang di turunkan. Simulated passenger drop-off timestamp.
        pickup_location_id: ID zona asal. NYC Taxi Zone where the trip begins.
        dropoff_location_id: ID zona tujuan. NYC Taxi Zone where the trip ends.
        passenger_count: Jumlah penumpang. Number of simulated passengers from one through six.
        trip_distance: Jarak perjalanan dalam mil. Simulated trip distance in miles.
        fare_amount: Tarif utama perjalanan. Base fare plus a distance-based simulated charge.
        tip_amount: Uang tip. Simulated tip, applied only to card payments.
        tolls_amount: Biaya tol. Simulated toll amount, which may be zero.
        total_amount:  Total seluruh biaya. Sum of fare, tip, toll, and fixed surcharge.
        payment_type: Kode metode pembayaran. Categorical payment code from one through four.
        source_type: Penanda bahwa data berasal dari streaming. Lineage marker identifying the record as streaming data.
    """

    event_id: str
    event_time: str
    ingestion_time: str
    pickup_datetime: str
    dropoff_datetime: str
    pickup_location_id: int
    dropoff_location_id: int
    passenger_count: int
    trip_distance: float
    fare_amount: float
    tip_amount: float
    tolls_amount: float
    total_amount: float
    payment_type: int
    source_type: str = "stream"

    def to_dict(self) -> dict[str, object]:
        """Mengubah event menjadi dictionary yang dapat diserialisasi ke JSON.

        Returns:
            Dictionary baru dengan key yang sesuai dengan skema event Pub/Sub.
        """

        # asdict menghasilkan mapping terpisah yang aman diubah oleh pemanggil.
        return asdict(self)


def _rfc3339(value: datetime) -> str:
    """Memformat datetime yang memiliki zona waktu menjadi string UTC RFC 3339.

    Args:
        value: Timestamp yang akan dinormalisasi ke UTC dan diserialisasi.

    Returns:
        String berakhiran ``Z`` yang dapat diproses BigQuery sebagai TIMESTAMP.
    """

    # utc_value menyeragamkan zona waktu input agar semua event memakai standar UTC.
    utc_value = value.astimezone(timezone.utc)

    # formatted_value mengganti akhiran +00:00 milik Python menjadi Z sesuai RFC 3339.
    formatted_value = utc_value.isoformat().replace("+00:00", "Z")
    return formatted_value


def generate_event(
    random_generator: random.Random | None = None,
) -> TaxiEvent:
    """Membuat satu event taksi realistis dan valid untuk Juni–Juli 2025.

    Args:
        random_generator: Generator acak dengan seed yang bersifat opsional
            untuk pengujian deterministik. Generator baru tanpa seed digunakan
            ketika argumen ini tidak diberikan.

    Returns:
        Objek TaxiEvent yang valid berdasarkan proses pembuatannya dan siap
        dikonversi menjadi JSON.
    """

    # rng adalah sumber angka acak yang aktif; pengujian dapat memasukkan seed
    # agar hasil yang dihasilkan dapat diulang.
    rng = random_generator or random.Random()

    # latest_pickup menentukan waktu pickup terakhir agar perjalanan dengan
    # durasi maksimum tetap selesai sebelum batas awal Agustus.
    latest_pickup = EVENT_END - timedelta(minutes=MAX_TRIP_MINUTES)

    # available_seconds adalah jumlah offset detik pickup yang valid
    # dalam periode dua bulan.
    available_seconds = int(
        (latest_pickup - EVENT_START).total_seconds()
    )

    # pickup adalah timestamp kejadian bisnis yang dibuat secara acak
    # dan berada di dalam Juni atau Juli 2025.
    pickup = EVENT_START + timedelta(
        seconds=rng.randrange(available_seconds)
    )

    # sampled_duration menggunakan distribusi log-normal agar perjalanan
    # berdurasi pendek lebih sering dihasilkan.
    sampled_duration = round(
        rng.lognormvariate(2.7, 0.55)
    )

    # duration_minutes menerapkan aturan batas minimum dan maksimum durasi.
    duration_minutes = max(
        MIN_TRIP_MINUTES,
        min(MAX_TRIP_MINUTES, sampled_duration),
    )

    # dropoff terjadi setelah pickup dan tetap berada sebelum awal Agustus.
    dropoff = min(
        pickup + timedelta(minutes=duration_minutes),
        EVENT_END - timedelta(seconds=1),
    )

    # average_speed_mph menyimulasikan kecepatan berkendara rata-rata
    # yang masuk akal untuk perjalanan di wilayah perkotaan.
    average_speed_mph = rng.uniform(8.0, 24.0)

    # raw_distance menghitung jarak berdasarkan durasi perjalanan dan
    # kecepatan sehingga kedua nilai tersebut saling berkaitan.
    raw_distance = duration_minutes / 60 * average_speed_mph

    # trip_distance menerapkan batas minimum dan maksimum jarak,
    # kemudian membulatkannya menjadi dua angka desimal.
    trip_distance = round(
        min(
            max(MIN_DISTANCE_MILES, raw_distance),
            MAX_DISTANCE_MILES,
        ),
        2,
    )

    # mileage_rate_dollars memberikan variasi pada tarif simulasi
    # yang dikenakan untuk setiap mil perjalanan.
    mileage_rate_dollars = rng.uniform(2.2, 3.4)

    # fare_amount menggabungkan tarif dasar tetap dan biaya berdasarkan
    # jarak perjalanan dalam dolar AS.
    fare_amount = round(
        BASE_FARE_DOLLARS
        + trip_distance * mileage_rate_dollars,
        2,
    )

    # payment_type menggunakan bobot yang telah ditentukan agar distribusinya
    # menyerupai perilaku pembayaran yang umum.
    payment_type = rng.choices(
        PAYMENT_TYPES,
        weights=PAYMENT_WEIGHTS,
        k=1,
    )[0]

    # tip_rate hanya diterapkan pada payment type 1 yang merepresentasikan
    # pembayaran menggunakan kartu.
    tip_rate = (
        rng.uniform(0.10, 0.25)
        if payment_type == 1
        else 0.0
    )

    # tip_amount menyimpan nilai tip simulasi dengan presisi dua angka desimal.
    tip_amount = round(
        fare_amount * tip_rate,
        2,
    )

    # tolls_amount biasanya bernilai nol dan sesekali menggunakan
    # nilai biaya tol yang telah ditentukan.
    tolls_amount = rng.choice(
        (0.0, 0.0, 0.0, TOLL_CHARGE_DOLLARS)
    )

    # total_amount adalah keseluruhan biaya simulasi yang dibayar penumpang.
    total_amount = round(
        fare_amount
        + tip_amount
        + tolls_amount
        + SURCHARGE_DOLLARS,
        2,
    )

    # passenger_count menggunakan distribusi berbobot dan tetap berada
    # dalam rentang satu hingga enam penumpang.
    passenger_count = rng.choices(
        PASSENGER_COUNTS,
        weights=PASSENGER_WEIGHTS,
        k=1,
    )[0]

    # ingestion_time mencatat waktu aktual ketika data simulasi
    # perjalanan historis ini dibuat.
    ingestion_time = datetime.now(timezone.utc)

    # taxi_event menggabungkan seluruh field yang telah ditentukan
    # ke dalam kontrak event yang immutable.
    taxi_event = TaxiEvent(
        event_id=str(uuid.uuid4()),
        event_time=_rfc3339(pickup),
        ingestion_time=_rfc3339(ingestion_time),
        pickup_datetime=_rfc3339(pickup),
        dropoff_datetime=_rfc3339(dropoff),
        pickup_location_id=rng.randint(
            MIN_LOCATION_ID,
            MAX_LOCATION_ID,
        ),
        dropoff_location_id=rng.randint(
            MIN_LOCATION_ID,
            MAX_LOCATION_ID,
        ),
        passenger_count=passenger_count,
        trip_distance=trip_distance,
        fare_amount=fare_amount,
        tip_amount=tip_amount,
        tolls_amount=tolls_amount,
        total_amount=total_amount,
        payment_type=payment_type,
    )

    return taxi_event
   