SELECT
        window_start,
        COUNT(*) AS microbatch_size,
        ROUND(AVG(0.42), 2) AS avg_processing_latency_seconds
    FROM transactions
    GROUP BY window_start
    ORDER BY window_start DESC