-- Query Analitik 1:
-- Merangkum performa data batch aktual April–Mei 2025.

SELECT
    pickup_month,             -- Bulan perjalanan.
    trip_count,               -- Jumlah perjalanan valid dan unik.
    total_revenue,            -- Total pendapatan dalam dolar.
    average_total_amount,     -- Rata-rata total biaya per perjalanan.
    average_trip_distance     -- Rata-rata jarak perjalanan dalam mil.

FROM `jcdeah-009.taufiqzahrus_capstone3.mart_monthly_taxi_performance`

-- Hanya mengambil data batch aktual dan mengecualikan stream simulasi.
WHERE source_type = 'batch'

-- Mengurutkan April sebelum Mei.
ORDER BY pickup_month;