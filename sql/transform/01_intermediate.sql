-- Tujuan: menyeragamkan, menggabungkan, memvalidasi, dan menghapus
-- duplikasi data perjalanan batch dan streaming.
-- Project: jcdeah-009 | Dataset: taufiqzahrus_capstone3
-- CREATE OR REPLACE membuat transformasi bersifat idempoten
-- setiap kali dijalankan ulang secara manual.
CREATE OR REPLACE TABLE `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`

-- Partisi berdasarkan pickup_date mengurangi jumlah byte yang dipindai
-- ketika query menggunakan filter tanggal April–Juli.
PARTITION BY pickup_date

-- Clustering mendukung perbandingan sumber data, analisis zona,
-- dan analisis metode pembayaran.
CLUSTER BY source_type, pickup_location_id, payment_type AS

WITH unified AS (
    -- Jalur batch: memetakan nama kolom asli NYC TLC
    -- ke dalam struktur standar data warehouse.
    SELECT
        -- trip_id membuat fingerprint dari nilai sumber yang stabil
        -- agar data duplikat dapat dihapus ketika pipeline dijalankan ulang.
        TO_HEX(
            SHA256(
                CONCAT(
                    CAST(lpep_pickup_datetime AS STRING), '|',
                    CAST(lpep_dropoff_datetime AS STRING), '|',
                    CAST(PULocationID AS STRING), '|',
                    CAST(DOLocationID AS STRING), '|',
                    CAST(trip_distance AS STRING), '|',
                    CAST(total_amount AS STRING)
                )
            )
        ) AS trip_id,

        -- Waktu ketika penumpang dijemput.
        lpep_pickup_datetime AS pickup_datetime,

        -- Waktu ketika penumpang diturunkan.
        lpep_dropoff_datetime AS dropoff_datetime,

        -- Tanggal bisnis yang siap digunakan sebagai partisi.
        DATE(lpep_pickup_datetime) AS pickup_date,

        -- ID Taxi Zone lokasi asal.
        CAST(PULocationID AS INT64) AS pickup_location_id,

        -- ID Taxi Zone lokasi tujuan.
        CAST(DOLocationID AS INT64) AS dropoff_location_id,

        -- Menggunakan nilai default 1 jika jumlah penumpang bernilai NULL.
        COALESCE(CAST(passenger_count AS INT64), 1) AS passenger_count,

        -- Jarak perjalanan dalam mil.
        CAST(trip_distance AS FLOAT64) AS trip_distance,

        -- Tarif perjalanan berdasarkan meter dalam dolar.
        CAST(fare_amount AS FLOAT64) AS fare_amount,

        -- Jumlah tip yang tercatat dalam dolar.
        CAST(tip_amount AS FLOAT64) AS tip_amount,

        -- Jumlah biaya tol yang tercatat dalam dolar.
        CAST(tolls_amount AS FLOAT64) AS tolls_amount,

        -- Total keseluruhan biaya yang dibebankan.
        CAST(total_amount AS FLOAT64) AS total_amount,

        -- Kode kategori metode pembayaran NYC.
        CAST(payment_type AS INT64) AS payment_type,

        -- Label asal data untuk file sumber April–Mei.
        'batch' AS source_type,

        -- Waktu ketika transformasi batch ini dijalankan.
        CURRENT_TIMESTAMP() AS ingestion_time

    FROM `jcdeah-009.taufiqzahrus_capstone3.stg_green_taxi_batch`

    -- Membatasi data batch hanya untuk periode April–Mei
    -- serta menerapkan aturan utama kualitas data.
    WHERE DATE(lpep_pickup_datetime)
        BETWEEN '2025-04-01' AND '2025-05-31'
      AND lpep_dropoff_datetime > lpep_pickup_datetime
      AND PULocationID BETWEEN 1 AND 265
      AND DOLocationID BETWEEN 1 AND 265
      AND trip_distance > 0
      AND fare_amount >= 0
      AND total_amount > 0

    UNION ALL

    -- Jalur streaming: memilih event Juni–Juli yang telah divalidasi
    -- oleh Dataflow dengan urutan kolom yang sama seperti data batch.
    SELECT
        -- UUID yang dibuat oleh publisher Python.
        event_id AS trip_id,

        -- Timestamp pickup historis yang disimulasikan.
        pickup_datetime,

        -- Timestamp drop-off historis yang disimulasikan.
        dropoff_datetime,

        -- Tanggal yang diturunkan oleh Dataflow dari pickup_datetime.
        pickup_date,

        -- ID Taxi Zone asal yang telah divalidasi.
        pickup_location_id,

        -- ID Taxi Zone tujuan yang telah divalidasi.
        dropoff_location_id,

        -- Jumlah penumpang yang telah divalidasi dalam rentang 1–6.
        passenger_count,

        -- Jarak perjalanan positif dalam mil yang telah divalidasi.
        trip_distance,

        -- Tarif dasar nonnegatif yang telah divalidasi.
        fare_amount,

        -- Jumlah tip nonnegatif yang telah divalidasi.
        tip_amount,

        -- Jumlah biaya tol nonnegatif yang telah divalidasi.
        tolls_amount,

        -- Total biaya positif yang telah divalidasi.
        total_amount,

        -- Kategori pembayaran yang telah divalidasi dalam rentang 1–4.
        payment_type,

        -- Label asal data untuk record Pub/Sub dan Dataflow.
        'stream' AS source_type,

        -- Timestamp generator aktual yang digunakan untuk pemeriksaan freshness.
        ingestion_time

    FROM `jcdeah-009.taufiqzahrus_capstone3.stg_green_taxi_stream`

    -- Memastikan kembali bahwa event historis berada dalam periode
    -- Juni–Juli sesuai persyaratan mentor.
    WHERE pickup_date BETWEEN '2025-06-01' AND '2025-07-31'
),

ranked AS (
    -- ROW_NUMBER memberikan peringkat pada trip_id yang duplikat
    -- berdasarkan waktu ingestion terbaru.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY ingestion_time DESC
        ) AS row_number
    FROM unified
)

-- Mempertahankan hanya record terbaru untuk setiap trip_id
-- dan mengecualikan kolom sementara row_number dari hasil akhir.
SELECT * EXCEPT (row_number)
FROM ranked
WHERE row_number = 1;