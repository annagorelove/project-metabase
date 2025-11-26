WITH purchases AS (
    SELECT 
        u.user_id,
        p.cost AS coins_spent,
        'Покупка задачи' AS category
    FROM CodeSubmit u
    JOIN Problem p ON u.problem_id = p.id
    [[WHERE u.created_at BETWEEN {{date}}]]
    UNION ALL
    SELECT 
        u.user_id,
        p.solution_cost AS coins_spent,
        'Покупка решений' AS category
    FROM CodeSubmit u
    JOIN Problem p ON u.problem_id = p.id
    [[WHERE u.created_at BETWEEN {{date}}]]
    UNION ALL
    SELECT 
        p.user_id,
        t.cost AS coins_spent,
        'Покупка тестов' AS category
    FROM test t
    JOIN TestResult p ON p.test_id = t.id
    [[WHERE p.created_at BETWEEN {{date}}]]
    UNION ALL
    SELECT 
        p.user_id,
        t.explanation_cost AS coins_spent,
        'Покупка подсказок' AS category
    FROM TestQuestion t
    JOIN TestResult p ON p.test_id = t.test_id
    [[WHERE p.created_at BETWEEN {{date}}]]
)
SELECT 
    category,
    COUNT(DISTINCT user_id) AS "уникальные юзеры",
    SUM(coins_spent) AS "общее кол-во затраченных коинов",
    ROUND(100.0 * SUM(coins_spent) / SUM(SUM(coins_spent)) OVER (), 2) AS "процентное соотношение"
FROM purchases
GROUP BY category
ORDER BY "процентное соотношение" DESC;
