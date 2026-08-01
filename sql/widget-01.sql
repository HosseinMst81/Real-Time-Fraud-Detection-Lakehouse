SELECT window_start,
    SUM(
        CASE
            WHEN prediction = 0 THEN 1
            ELSE 0
        END
    ) AS normal_transactions,
    SUM(
        CASE
            WHEN prediction = 1 THEN 1
            ELSE 0
        END
    ) AS fraud_transactions,
    COUNT(*) AS total_volume
FROM transactions
GROUP BY window_start
ORDER BY window_start DESC;