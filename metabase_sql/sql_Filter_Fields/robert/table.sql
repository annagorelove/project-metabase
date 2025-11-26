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
    SELECT event, 'discount=' || discount_filled AS param
    FROM filled
    WHERE discount_filled IS NOT NULL
    UNION ALL
    SELECT event, 'utm_source=' || utm_source_filled
    FROM filled
    WHERE utm_source_filled IS NOT NULL
    UNION ALL
    SELECT event, 'funnel=' || funnel_filled
    FROM filled
    WHERE funnel_filled IS NOT NULL
    UNION ALL
    SELECT event, 'source=' || source_filled
    FROM filled
    WHERE source_filled IS NOT NULL
)
SELECT
    param AS params,
    event,
    COUNT(*) AS cnt
FROM unnested
GROUP BY params, event
ORDER BY cnt DESC;


