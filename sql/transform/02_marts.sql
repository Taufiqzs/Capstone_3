-- Tujuan: membangun mart analitik taksi harian dan bulanan
-- yang dilengkapi dokumentasi.
-- Project: jcdeah-009 | Dataset: taufiqzahrus_capstone3

-- Mart harian mendukung analisis tren dan perbandingan sumber data
-- pada tingkat tanggal.
CREATE OR REPLACE TABLE
    `jcdeah-009.taufiqzahrus_capstone3.mart_daily_taxi_summary`

-- Partisi berdasarkan pickup_date mengurangi pemindaian data
-- ketika query menggunakan rentang tanggal.
PARTITION BY pickup_date

-- Clustering berdasarkan source_type mengelompokkan record batch
-- dan stream agar perbandingan sumber data lebih efisien.
CLUSTER BY source_type AS

SELECT
    -- Tanggal kalender ketika penumpang dijemput.
    pickup_date,

    -- Asal data: batch untuk April–Mei
    -- atau stream untuk Juni–Juli.
    source_type,

    -- Jumlah perjalanan bersih dan unik
    -- untuk setiap tanggal dan sumber data.
    COUNT(*) AS trip_count,

    -- Total pendapatan yang dibebankan dalam dolar.
    ROUND(SUM(total_amount), 2) AS total_revenue,

    -- Rata-rata total biaya untuk setiap perjalanan.
    ROUND(AVG(total_amount), 2) AS average_total_amount,

    -- Rata-rata jarak perjalanan dalam mil.
    ROUND(AVG(trip_distance), 2) AS average_trip_distance,

    -- Rata-rata durasi perjalanan dalam menit.
    ROUND(
        AVG(
            TIMESTAMP_DIFF(
                dropoff_datetime,
                pickup_datetime,
                SECOND
            )
        ) / 60,
        2
    ) AS average_duration_minutes

FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`

-- Mengelompokkan hasil berdasarkan tanggal pickup
-- dan sumber data.
GROUP BY pickup_date, source_type;


-- Mart bulanan menyediakan ringkasan performa
-- untuk periode April hingga Juli.
CREATE OR REPLACE TABLE
    `jcdeah-009.taufiqzahrus_capstone3.mart_monthly_taxi_performance` AS

SELECT
    -- Tanggal pertama dari bulan yang dilaporkan.
    DATE_TRUNC(pickup_date, MONTH) AS pickup_month,

    -- Asal data untuk membantu interpretasi perbandingan
    -- antara batch dan streaming secara tepat.
    source_type,

    -- Jumlah perjalanan bersih dan unik
    -- untuk setiap bulan dan sumber data.
    COUNT(*) AS trip_count,

    -- Total pendapatan bulanan yang dibebankan dalam dolar.
    ROUND(SUM(total_amount), 2) AS total_revenue,

    -- Rata-rata total biaya untuk setiap perjalanan.
    ROUND(AVG(total_amount), 2) AS average_total_amount,

    -- Rata-rata jarak perjalanan dalam mil.
    ROUND(AVG(trip_distance), 2) AS average_trip_distance

FROM `jcdeah-009.taufiqzahrus_capstone3.int_green_taxi_trips`

-- Mengelompokkan hasil berdasarkan bulan pickup
-- dan sumber data.
GROUP BY pickup_month, source_type;