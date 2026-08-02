SELECT
        window_start,
        COUNT(*) AS total_tx,
        SUM(CASE WHEN prediction = 1.0 THEN 1 ELSE 0 END) AS fraud_tx,
        ROUND((SUM(CASE WHEN prediction = 1.0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS fraud_rate_percentage
    FROM transactions
    GROUP BY window_start
    ORDER BY window_start DESC