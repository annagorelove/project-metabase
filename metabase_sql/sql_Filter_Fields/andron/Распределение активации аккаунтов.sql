SELECT
  FLOOR(("public"."users"."is_active" / 0.125)) * 0.125 AS "is_active",
  COUNT(*) AS "count"
FROM
  "public"."users"
GROUP BY
  FLOOR(("public"."users"."is_active" / 0.125)) * 0.125
ORDER BY
  FLOOR(("public"."users"."is_active" / 0.125)) * 0.125 ASC