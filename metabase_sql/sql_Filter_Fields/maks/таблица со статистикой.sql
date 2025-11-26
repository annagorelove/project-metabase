WITH entries AS (
    SELECT 
        DATE_TRUNC('month', ue.entry_at) AS month_start,
        DATE_TRUNC('quarter', ue.entry_at) AS quarter_start,
        EXTRACT(MONTH FROM ue.entry_at) AS month_num,
        EXTRACT(QUARTER FROM ue.entry_at) AS quarter_num,
        EXTRACT(YEAR FROM ue.entry_at) AS year_num,
        COUNT(*) AS total_entries,
        COUNT(DISTINCT ue.user_id) AS unique_users
    FROM UserEntry ue
    [[WHERE EXTRACT(YEAR FROM ue.entry_at) = {{year}}]]
    GROUP BY month_start, quarter_start, month_num, quarter_num, year_num
),
attempts AS (
    SELECT 
        DATE_TRUNC('month', cs.created_at) AS month_start,
        DATE_TRUNC('quarter', cs.created_at) AS quarter_start,
        EXTRACT(MONTH FROM cs.created_at) AS month_num,
        EXTRACT(QUARTER FROM cs.created_at) AS quarter_num,
        EXTRACT(YEAR FROM cs.created_at) AS year_num,
        COUNT(*) AS total_attempts,
        COUNT(*) FILTER (WHERE cs.is_false = 0) AS successful_attempts,
        COUNT(DISTINCT cs.problem_id) FILTER (WHERE cs.is_false = 0) AS solved_problems
    FROM CodeSubmit cs
    [[WHERE EXTRACT(YEAR FROM cs.created_at) = {{year}}]]
    GROUP BY month_start, quarter_start, month_num, quarter_num, year_num
),
merged AS (
    SELECT 
        e.month_num,
        e.quarter_num,
        e.year_num,
        e.total_entries,
        e.unique_users,
        a.total_attempts,
        a.successful_attempts,
        a.solved_problems
    FROM entries e
    LEFT JOIN attempts a 
        ON e.month_num = a.month_num 
        AND e.year_num = a.year_num
)
SELECT metric as "Название",
    SUM(CASE WHEN month_num = 1 THEN value END) AS "Январь",
    SUM(CASE WHEN month_num = 2 THEN value END) AS "Февраль",
    SUM(CASE WHEN month_num = 3 THEN value END) AS "Март",
    SUM(CASE WHEN quarter_num = 1 THEN value END) AS "Q1",
    SUM(CASE WHEN month_num = 4 THEN value END) AS "Апрель",
    SUM(CASE WHEN month_num = 5 THEN value END) AS "Май",
    SUM(CASE WHEN month_num = 6 THEN value END) AS "Июнь",
    SUM(CASE WHEN quarter_num = 2 THEN value END) AS "Q2",
    SUM(CASE WHEN month_num = 7 THEN value END) AS "Июль",
    SUM(CASE WHEN month_num = 8 THEN value END) AS "Август",
    SUM(CASE WHEN month_num = 9 THEN value END) AS "Сентябрь",
    SUM(CASE WHEN quarter_num = 3 THEN value END) AS "Q3",
    SUM(CASE WHEN month_num = 10 THEN value END) AS "Октябрь",
    SUM(CASE WHEN month_num = 11 THEN value END) AS "Ноябрь",
    SUM(CASE WHEN month_num = 12 THEN value END) AS "Декабрь",
    SUM(CASE WHEN quarter_num = 4 THEN value END) AS "Q4"
FROM (
    SELECT 'Всего заходов на платформу' AS metric, month_num, quarter_num, total_entries AS value FROM merged
    UNION ALL
    SELECT 'Уникальных пользователей', month_num, quarter_num, unique_users FROM merged
    UNION ALL
    SELECT 'Попыткок решения задач', month_num, quarter_num, total_attempts FROM merged
    UNION ALL
    SELECT 'Успешных попыток решения задач', month_num, quarter_num, successful_attempts FROM merged
    UNION ALL
    SELECT 'Успешно решённых задач', month_num, quarter_num, solved_problems FROM merged
) t
GROUP BY metric
ORDER BY metric;
