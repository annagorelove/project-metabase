WITH parsed AS (
    SELECT
        s.uid,
        s.event,
        s.created_at,
        NULLIF(
            regexp_replace(
                regexp_replace(s.url_params, '^\\?', ''), 
                '.*discount=([^&]+).*', '\1'
            ),
            s.url_params
        ) AS discount,
        NULLIF(
            regexp_replace(
                regexp_replace(s.url_params, '^\\?', ''), 
                '.*utm_source=([^&]+).*', '\1'
            ),
            s.url_params
        ) AS utm_source,
        NULLIF(
            regexp_replace(
                regexp_replace(s.url_params, '^\\?', ''), 
                '.*funnel=([^&]+).*', '\1'
            ),
            s.url_params
        ) AS funnel,
        NULLIF(
            regexp_replace(
                regexp_replace(s.url_params, '^\\?', ''), 
                '.*source=([^&]+).*', '\1'
            ),
            s.url_params
        ) AS source
    FROM stat s
    WHERE {{date}}
),
filled AS (
    SELECT
        uid,
        event,
        created_at,
        COALESCE(
            discount,
            LAST_VALUE(discount) OVER (
                PARTITION BY uid
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        ) AS discount_filled,
        COALESCE(
            utm_source,
            LAST_VALUE(utm_source) OVER (
                PARTITION BY uid
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        ) AS utm_source_filled,
        COALESCE(
            funnel,
            LAST_VALUE(funnel) OVER (
                PARTITION BY uid
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        ) AS funnel_filled,
        COALESCE(
            source,
            LAST_VALUE(source) OVER (
                PARTITION BY uid
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        ) AS source_filled
    FROM parsed
),
unnested AS (
    SELECT uid, event, 'discount=' || discount_filled AS param
    FROM filled
    WHERE discount_filled IS NOT NULL
    UNION ALL
    SELECT uid, event, 'utm_source=' || utm_source_filled
    FROM filled
    WHERE utm_source_filled IS NOT NULL
    UNION ALL
    SELECT uid, event, 'funnel=' || funnel_filled
    FROM filled
    WHERE funnel_filled IS NOT NULL
    UNION ALL
    SELECT uid, event, 'source=' || source_filled
    FROM filled
    WHERE source_filled IS NOT NULL
),
events AS (
    SELECT
        uid,
        param,
        MAX(CASE WHEN event = 'enter_site' THEN 1 ELSE 0 END) AS step1_visit,
        MAX(CASE WHEN event = 'open_page' THEN 1 ELSE 0 END) AS step2_page,
        MAX(CASE WHEN event = 'request_consultation' THEN 1 ELSE 0 END) AS step3_consult,
        MAX(CASE WHEN event = 'request_demo' THEN 1 ELSE 0 END) AS step4_demo,
        MAX(CASE WHEN event = 'go_to__payment' THEN 1 ELSE 0 END) AS step5_payment,
        MAX(CASE WHEN event = 'get_test_results' THEN 1 ELSE 0 END) AS step6_results
    FROM unnested
    GROUP BY uid, param
),
counts AS (
    SELECT param, '1. Зашел на сайт' AS step, COUNT(*) AS users_count FROM events WHERE step1_visit = 1
    GROUP BY param
    UNION ALL
    SELECT param, '2. Открыл страницу', COUNT(*) FROM events WHERE step2_page = 1 GROUP BY param
    UNION ALL
    SELECT param, '3. Оставил заявку на консультацию', COUNT(*) FROM events WHERE step3_consult = 1 GROUP BY param
    UNION ALL
    SELECT param, '4. Запросил демо', COUNT(*) FROM events WHERE step4_demo = 1 GROUP BY param
    UNION ALL
    SELECT param, '5. Перешел к оплате', COUNT(*) FROM events WHERE step5_payment = 1 GROUP BY param
    UNION ALL
    SELECT param, '6. Запросил результаты теста', COUNT(*) FROM events WHERE step6_results = 1 GROUP BY param
),
with_prev AS (
    SELECT
        param,
        step,
        users_count,
        LAG(users_count) OVER (PARTITION BY param ORDER BY step) AS prev_count
    FROM counts
)
SELECT
    step,
    users_count,
    CASE 
        WHEN prev_count IS NULL THEN 100.0
        ELSE ROUND(users_count::numeric / NULLIF(prev_count,0) * 100, 2)
    END AS conversion_percent
FROM with_prev
WHERE 1=1
  [[AND param ILIKE {{param}}]]
ORDER BY step;