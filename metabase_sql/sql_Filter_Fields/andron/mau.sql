SELECT
  DATE_TRUNC('month', "public"."users"."date_joined") AS "date_joined",
  COUNT(*) AS "count"
FROM
  "public"."users"
WHERE
  "public"."users"."id" IS NOT NULL
GROUP BY
  DATE_TRUNC('month', "public"."users"."date_joined")
ORDER BY
  DATE_TRUNC('month', "public"."users"."date_joined") ASC