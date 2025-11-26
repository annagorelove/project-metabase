-- Из таблицы bonuscheques берем все необходимые поля для RFM-анализа, так же добавляем два фильтра
WITH transactions AS (
    SELECT
        bonuscheques.card AS client_id,
        bonuscheques.doc_id,
        bonuscheques.shop,
        bonuscheques.datetime :: DATE AS transaction_date,
        MAX(bonuscheques.datetime :: DATE) OVER () AS current_dt,
        bonuscheques.summ_with_disc AS purchase_amount
      FROM bonuscheques 
      JOIN shops ON shops.name = bonuscheques .shop
      WHERE 
        bonuscheques.card SIMILAR TO '200%'
        [[AND {{date}}]]                      
        [[AND {{shops}}]]  
),
-- Считаем recency, frequency и monetary
rfm_base AS (
    SELECT 
        client_id,
        shop,
        MIN(current_dt - transaction_date) AS recency,
        COUNT(DISTINCT doc_id) AS frequency,
        SUM(purchase_amount) AS monetary
    FROM transactions
    GROUP BY client_id, shop
),
-- Рассчитываем пороговые значения с помощью перцентилей
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY recency) AS r25,
        PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY recency) AS recency_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY recency) AS r75,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY frequency) AS f25,
        PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY frequency) AS frequency_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY frequency) AS f75,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY monetary) AS m25,
        PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY monetary) AS monetary_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY monetary) AS m75
    FROM rfm_base
),
-- Присваиваем каждому перцентилю группу
rfm_ranked AS (
    SELECT
        r.client_id,
        r.shop,
        CASE 
            WHEN recency <= p.r25 THEN 0
            WHEN recency <= p.recency_median THEN 1
            WHEN recency <= p.r75 THEN 2
            ELSE 3
        END AS r_rank,
        CASE 
            WHEN frequency <= p.f25 THEN 3
            WHEN frequency <= p.frequency_median THEN 2
            WHEN frequency <= p.f75 THEN 1
            ELSE 0
        END AS f_rank,
        CASE 
            WHEN monetary <= p.m25 THEN 3
            WHEN monetary <= p.monetary_median THEN 2
            WHEN monetary <= p.m75 THEN 1
            ELSE 0
        END AS m_rank
    FROM rfm_base r
    CROSS JOIN percentiles p
),
-- Соединяем посчитанные группы в один целый RFM сегмент
all_rfm as (
    SELECT
        client_id,
        shop,
        CONCAT(r_rank, f_rank, m_rank) AS rfm_code
    FROM rfm_ranked
),
-- Даем каждому сегменту название исходя из его показателей
rfm_final AS (
    SELECT
        client_id,
        rfm_code,
        shop,
        CASE
            WHEN rfm_code IN ('000') THEN 'Чемпионы'
            WHEN rfm_code IN ('001', '110', '111', '101', '011', '100', '010') THEN 'Потенциальные лидеры'
            WHEN rfm_code IN ('122', '123','023', '013', '022', '112', '113', '012', '002', '003', '102', '103') THEN 'Лояльные'
            WHEN rfm_code IN ('021', '031', '020', '030', '121', '131', '120', '130', '201') THEN 'Перспективные'
            WHEN rfm_code IN ('210', '220', '211', '221', '230') THEN 'Низкоактивные крупные покупатели'
            WHEN rfm_code IN ('222', '212', '213', '223', '202', '203') THEN 'Растущие'
            WHEN rfm_code IN ('133', '132', '033', '032') THEN 'Новички'
            WHEN rfm_code IN ('330','331', '320', '310', '321', '311', '200', '300', '301', '231') THEN 'Требуют внимания'
            WHEN rfm_code IN ('303', '302') THEN 'Сомневаются'
            WHEN rfm_code IN ('313', '312') THEN 'В зоне риска'
            WHEN rfm_code IN ('322', '323', '232', '233') THEN 'Спящие'
            WHEN rfm_code IN ('332', '333') THEN 'Потерянные'
            ELSE 'Прочие'
        END AS rfm_group
    FROM all_rfm
),
-- Находим когда была совершенная первая покупка каждым клиентом
first_tx AS (
    SELECT 
        t.client_id,
        r.rfm_group,
        MIN(t.transaction_date) AS first_purchase
    FROM transactions t
    JOIN rfm_final r 
    ON r.client_id = t.client_id
    GROUP BY t.client_id, r.rfm_group
),
-- Делим клиентов на когорты по их первой покупке
tx_with_month AS (
    SELECT
        t.client_id,
        f.rfm_group,
        f.first_purchase,
        DATE_TRUNC('month', t.transaction_date) AS tx_month,
        DATE_TRUNC('month', f.first_purchase) AS cohort_month
    FROM transactions t
    JOIN first_tx f ON t.client_id = f.client_id
),
-- Считаем интервал между месяцем первой покупки и датой транзакции
retention_period AS (
    SELECT
        client_id,
        rfm_group,
        cohort_month,
        AGE(tx_month, cohort_month) AS period_age
    FROM tx_with_month
),
-- Считаем сколько прошло месяцев после первой покупки
retention_period_months AS (
    SELECT
        client_id,
        cohort_month,
        rfm_group,
        DATE_PART('year', period_age) * 12 + DATE_PART('month', period_age) AS months_after_first
    FROM retention_period
), 
-- Находим сколько в каждой когорте уникальных клиентов
cohort_size AS (
    SELECT cohort_month, 
           rfm_group,
           COUNT(DISTINCT client_id) AS cohort_users
    FROM retention_period_months
    WHERE months_after_first = 0
    GROUP BY cohort_month, rfm_group
)
-- Рассчитываем коэффициент удержания и выводим все необходимые поля
SELECT
    r.cohort_month AS "месяц",
    r.rfm_group AS "группы",
    r.retained_users AS "вернулось",
    c.cohort_users AS "всего покупателей",
    ROUND(LEAST(r.retained_users::numeric, c.cohort_users) / NULLIF(c.cohort_users, 0), 2) AS "коэффициент удержания"
FROM (
    SELECT
        cohort_month,
        rfm_group,
        months_after_first,
        COUNT(DISTINCT client_id) AS retained_users
    FROM retention_period_months
    GROUP BY cohort_month, months_after_first, rfm_group
) r
JOIN cohort_size c 
  ON r.cohort_month = c.cohort_month
 AND r.rfm_group = c.rfm_group
WHERE 1=1
  [[AND r.rfm_group = {{rfm_group}}]]
ORDER BY r.cohort_month, r.months_after_first, r.rfm_group;