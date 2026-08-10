-- Query Analitik 2:
-- Membandingkan data batch aktual dengan data streaming simulasi.

SELECT
    -- Kelompok asal data: batch atau stream.
    source_type,

    -- Total perjalanan bersih dari seluruh tanggal pada setiap sumber data.
    SUM(trip_count) AS trip_count,

    -- Total pendapatan yang dibebankan dalam dolar.
    ROUND(
        SUM(total_revenue),
        2
    ) AS total_revenue,

    -- Rata-rata pendapatan tertimbang untuk setiap perjalanan.
    -- SAFE_DIVIDE mencegah error apabila jumlah perjalanan bernilai nol.
    ROUND(
        SAFE_DIVIDE(
            SUM(total_revenue),
            SUM(trip_count)
        ),
        2
    ) AS revenue_per_trip,

    -- Rata-rata jarak perjalanan dalam mil yang dihitung menggunakan
    -- jumlah perjalanan sebagai bobot.
    ROUND(
        SAFE_DIVIDE(
            SUM(average_trip_distance * trip_count),
            SUM(trip_count)
        ),
        2
    ) AS average_trip_distance

FROM `jcdeah-009.taufiqzahrus_capstone3.mart_daily_taxi_summary`

-- Mengelompokkan hasil berdasarkan sumber data.
GROUP BY source_type

-- Mengurutkan hasil berdasarkan nama sumber data.
ORDER BY source_type;