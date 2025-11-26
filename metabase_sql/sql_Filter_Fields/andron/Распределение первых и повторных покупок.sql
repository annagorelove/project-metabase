WITH purchase_counts AS (
    SELECT 
        user_id,
        COUNT(*) AS total_purchases
    FROM transaction
    [[WHERE created_at BETWEEN {{date}}]]
    GROUP BY user_id
)
SELECT 
    CASE 
        WHEN total_purchases = 1 THEN 'Первая покупка'
        ELSE 'Повторная покупка'
    END AS purchase_type,
    COUNT(*) AS users_count
FROM purchase_counts
GROUP BY purchase_type
ORDER BY purchase_type;