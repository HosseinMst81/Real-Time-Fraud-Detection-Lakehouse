SELECT
        SUM(CASE WHEN prediction = 1.0 THEN Amount ELSE 0 END) AS total_fraud_amount_usd,
        AVG(CASE WHEN prediction = 1.0 THEN Amount ELSE NULL END) AS avg_fraud_amount_usd,
        SUM(CASE WHEN prediction = 0.0 THEN Amount ELSE 0 END) AS total_legit_amount_usd,
        SUM(Amount) AS total_processed_volume_usd
    FROM transactions