SELECT
  CAST("public"."users"."date_joined" AS date) AS "date_joined",
  COUNT(*) AS "count"
FROM
  "public"."users"
WHERE
  ("public"."users"."id" IS NOT NULL)
 
   AND (
    "public"."users"."date_joined" >= DATE_TRUNC('year', (NOW() + INTERVAL '-30 year'))
  )
  AND (
    "public"."users"."date_joined" < DATE_TRUNC('year', NOW())
  )
GROUP BY
  CAST("public"."users"."date_joined" AS date)
ORDER BY
  CAST("public"."users"."date_joined" AS date) ASC