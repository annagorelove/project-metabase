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
)
-- Выводим все в отдельную таблицу, добавляем фильтр по сегментам
SELECT r.rfm_group,
r.shop,
rf.monetary,
rf.recency, 
rf.frequency,
r.client_id,
r.rfm_code
FROM rfm_base rf
JOIN rfm_final r 
ON rf.client_id = r.client_id
WHERE 1=1
  [[AND r.rfm_group = {{rfm_group}}]]