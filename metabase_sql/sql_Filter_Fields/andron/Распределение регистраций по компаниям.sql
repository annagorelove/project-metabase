SELECT
  "public"."users"."company_id" AS "company_id",
  COUNT(*) AS "count"
FROM
  "public"."users"
WHERE
  (
    "public"."users"."date_joined" >= DATE_TRUNC('year', (NOW() + INTERVAL '-12 year'))
  )
 
   AND (
    "public"."users"."date_joined" < DATE_TRUNC('year', NOW())
  )
  AND ("public"."users"."company_id" IS NOT NULL)
GROUP BY
  "public"."users"."company_id"
ORDER BY
  "public"."users"."company_id" ASC