--- description: For each of the specified intervals, report conversion rate, average LCP, FID, CLS, INP, standard error, and if there is a statistically significant difference between the latest interval and the current interval
--- url: -
--- startdate: 2021-06-22
--- enddate: 2021-06-22
--- timezone: UTC
--- conversioncheckpoint: click
--- targets: https://, http://
--- intervals: 2023-06-22,#target,2023-05-01,#mayday
--- domainkey: secret
WITH prefixes AS (
  SELECT CONCAT(TRIM(prefix), "%") AS prefix
  FROM
    UNNEST(
      SPLIT(@targets, ",")
    ) AS prefix
),
intervals_input AS (
  SELECT prefix AS step
  FROM
    UNNEST(
      SPLIT(@intervals, ",")
    ) AS prefix
),
numbered_intervals AS (
  SELECT
  row_num,
  step AS current_val,
  LEAD(step) OVER(ORDER BY row_num ASC) AS next_val
  FROM (
SELECT  
  step,
  ROW_NUMBER() OVER() AS row_num
FROM intervals_input
)),
named_intervals AS (
SELECT 
  interval_name,
  interval_start,
  COALESCE(LEAD(interval_start) OVER(ORDER BY interval_start), CURRENT_TIMESTAMP()) AS interval_end
FROM (
SELECT 
  next_val AS interval_name,
  TIMESTAMP(current_val) AS interval_start,  
FROM numbered_intervals
WHERE MOD(row_num, 2) = 1
)
ORDER BY interval_start DESC)
SELECT * FROM named_intervals

# Work in progress, steal from rum-checkpoint-cwv-correlation
