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
-- Считаем дату следующей покупки для каждого клиента
purchases_with_next AS (
    SELECT
        t.client_id,
        t.transaction_date,
        LEAD(t.transaction_date) OVER (PARTITION BY t.client_id ORDER BY t.transaction_date) AS next_purchase_date
    FROM transactions t
),
-- Делим клиентов на группы исходя из риска ухода
churn_flag AS (
    SELECT
        p.client_id,
        CURRENT_DATE - p.transaction_date AS days_since_last_purchase,
        CASE 
            WHEN CURRENT_DATE - p.transaction_date <= 30 THEN 'До 30 дней'
            WHEN CURRENT_DATE - p.transaction_date > 90 THEN 'Более 90 дней'
            ELSE '31-90 дней'
        END AS recency_group,
        CASE 
            WHEN p.next_purchase_date IS NULL 
                 OR p.next_purchase_date > p.transaction_date + INTERVAL '90 days'
            THEN 1 ELSE 0
        END AS churn_risk
    FROM purchases_with_next p
)
-- Рассчитываем процент ухода по каждому сегменту
SELECT
    cf.recency_group,
    rf.rfm_group,
    COUNT(*) AS total_clients,
    SUM(cf.churn_risk) AS churn_clients,
    ROUND(100.0 * SUM(cf.churn_risk) / COUNT(*), 2) AS churn_percent
FROM churn_flag cf
JOIN rfm_final rf ON rf.client_id = cf.client_id
WHERE cf.recency_group IN ('До 30 дней', 'Более 90 дней')
[[AND rf.rfm_group = {{rfm_group}}]] 
GROUP BY cf.recency_group, rf.rfm_group
ORDER BY rf.rfm_group, churn_percent DESC;