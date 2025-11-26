SELECT
  "source"."Дата" AS "Дата",
  "source"."Прошло нед. после первого захода" AS "Прошло нед. после первого захода",
  "source"."Retention в процентах" AS "Retention в процентах"
FROM
  (
    WITH first_entry AS (
      SELECT
        user_id,
        MIN(entry_at) AS first_visit_date
      FROM
        UserEntry
     
GROUP BY
        user_id
    ),
    cohorts AS (
      SELECT
        user_id,
        DATE_TRUNC('month', first_visit_date) AS cohort_month,
        first_visit_date
      FROM
        first_entry
    ),
    activity AS (
      SELECT
        ue.user_id,
        c.cohort_month,
        c.first_visit_date,
        ue.entry_at,
        FLOOR(
          EXTRACT(
            EPOCH
            FROM
              (ue.entry_at - c.first_visit_date)
          ) / (7 * 24 * 60 * 60)
        ) AS weeks_since_first
      FROM
        UserEntry ue
        JOIN cohorts c ON ue.user_id = c.user_id
    ),
    rolling_retention AS (
      SELECT
        cohort_month,
        weeks_since_first,
        COUNT(DISTINCT user_id) AS active_users
      FROM
        activity
     
WHERE
        weeks_since_first >= 0
      GROUP BY
        cohort_month,
        weeks_since_first
    ),
    cohort_sizes AS (
      SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
      FROM
        cohorts
      GROUP BY
        cohort_month
    )
    SELECT
      r.cohort_month as "Дата",
      r.weeks_since_first as "Прошло нед. после первого захода",
      ROUND(100.0 * r.active_users / c.cohort_size, 2) AS "Retention в процентах"
    FROM
      rolling_retention r
      JOIN cohort_sizes c USING (cohort_month)
    WHERE
      weeks_since_first > 0
   
ORDER BY
      r.cohort_month,
      r.weeks_since_first
  ) AS "source"
LIMIT
  1048575