WITH
      registrations AS (
        SELECT
          u.user_id,
          u.created_at
        FROM
          CodeSubmit u [[WHERE u.created_at BETWEEN {{date}}]]
      ),
      first_orders AS (
        SELECT
          o.user_id,
          MIN(o.created_at) AS first_order_date
        FROM
          CodeSubmit o
        WHERE
          o.is_false = 1 [[AND o.created_at BETWEEN {{date}}]]
        GROUP BY
          o.user_id
      ),
      repeat_orders AS (
        SELECT
          o.user_id,
          MIN(o.created_at) AS repeat_order_date
        FROM
          TRANSACTION o
          JOIN first_orders fo ON fo.user_id = o.user_id
        WHERE
          (
            o.type_id BETWEEN 2 AND 22
            OR o.type_id = 29
          ) [[AND o.created_at BETWEEN {{date}}]]
        GROUP BY
          o.user_id
      ),
      counts AS (
        SELECT
          *
        FROM
          (
            SELECT
              1 AS stage_order,
              'Сделал попытку решить задачу' AS stage,
              COUNT(DISTINCT r.user_id) AS users_count
            FROM
              registrations r
            UNION ALL
            SELECT
              2,
              'Решил задачу успешно',
              COUNT(f.user_id)
            FROM
              first_orders f
            UNION ALL
            SELECT
              3,
              'Пополнил кошелек',
              COUNT(ro.user_id)
            FROM
              repeat_orders ro
          ) t
      ),
      with_conversion AS (
        SELECT
          stage_order,
          stage,
          users_count,
          ROUND(
            100.0 * users_count / LAG(users_count) OVER (
              ORDER BY
                stage_order
            ),
            1
          ) AS conversion_percent
        FROM
          counts
      )
    SELECT
      stage,
      users_count,
      conversion_percent
    FROM
      with_conversion
    ORDER BY
      stage_order