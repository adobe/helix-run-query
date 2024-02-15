--- description: Get page views for a given URL for a specified time period. You can control the reporting granularity and filter pageviews that have a specific checkpoint, source, and target.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2020-01-01
--- enddate: 2020-12-31
--- checkpoint: -
--- sources: -
--- targets: -
--- url: 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
WITH dailydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    pageviews,
    TIMESTAMP_TRUNC(time, DAY) AS trunc_date
  FROM helix_rum.EVENTS_V3(
    @url, # url
    CAST(@offset AS INT64), # offset
    CAST(@interval AS INT64), # days to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

weeklydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    pageviews,
    TIMESTAMP_TRUNC(time, ISOWEEK) AS trunc_date
  FROM helix_rum.EVENTS_V3(
    @url, # url
    CAST(@offset AS INT64) * 7, # offset in weeks
    CAST(@interval AS INT64) * 7, # weeks to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

monthlydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    pageviews,
    TIMESTAMP_TRUNC(time, MONTH) AS trunc_date
  FROM helix_rum.EVENTS_V3(
    @url, # url
    CAST(@offset AS INT64) * 30, # offset in months
    CAST(@interval AS INT64) * 30, # months to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

quarterlydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    pageviews,
    TIMESTAMP_TRUNC(time, QUARTER) AS trunc_date
  FROM helix_rum.EVENTS_V3(
    @url, # url
    CAST(@offset AS INT64) * 90, # offset in quarters
    CAST(@interval AS INT64) * 90, # quarters to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

yearlydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    pageviews,
    TIMESTAMP_TRUNC(time, YEAR) AS trunc_date
  FROM helix_rum.EVENTS_V3(
    @url, # url
    CAST(@offset AS INT64) * 365, # offset in years
    @interval * 365, # years to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

all_checkpoints AS (
  # Combine all the data, so that we have it according to desired granularity
  SELECT * FROM dailydata WHERE @granularity = 1
  UNION ALL
  SELECT * FROM weeklydata WHERE @granularity = 7
  UNION ALL
  SELECT * FROM monthlydata WHERE @granularity = 30
  UNION ALL
  SELECT * FROM quarterlydata WHERE @granularity = 90
  UNION ALL
  SELECT * FROM yearlydata WHERE @granularity = 365
),

source_target_picked_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(all_checkpoints.pageviews) AS pageviews,
    ANY_VALUE(trunc_date) AS trunc_date
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @checkpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@sources, ',')) AS prefix
      WHERE all_checkpoints.source LIKE CONCAT(TRIM(prefix), '%')
    )
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@targets, ',')) AS prefix
      WHERE all_checkpoints.target LIKE CONCAT(TRIM(prefix), '%')
    )
  GROUP BY all_checkpoints.id
),

source_picked_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews,
    ANY_VALUE(trunc_date) AS trunc_date
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @checkpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@sources, ',')) AS prefix
      WHERE all_checkpoints.source LIKE CONCAT(TRIM(prefix), '%')
    )
  GROUP BY all_checkpoints.id
),

target_picked_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews,
    ANY_VALUE(trunc_date) AS trunc_date
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @checkpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@targets, ',')) AS prefix
      WHERE all_checkpoints.target LIKE CONCAT(TRIM(prefix), '%')
    )
  GROUP BY all_checkpoints.id
),

loose_picked_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews,
    ANY_VALUE(trunc_date) AS trunc_date
  FROM all_checkpoints
  WHERE all_checkpoints.checkpoint = @checkpoint
  GROUP BY all_checkpoints.id
),

picked_checkpoints AS (
  SELECT * FROM loose_picked_checkpoints
  WHERE @sources = '-' AND @targets = '-' AND @checkpoint != '-'
  UNION ALL
  SELECT * FROM source_target_picked_checkpoints
  WHERE @sources != '-' AND @targets != '-' AND @checkpoint != '-'
  UNION ALL
  SELECT * FROM source_picked_checkpoints
  WHERE @sources != '-' AND @targets = '-' AND @checkpoint != '-'
  UNION ALL
  SELECT * FROM target_picked_checkpoints
  WHERE @sources = '-' AND @targets != '-' AND @checkpoint != '-'
  UNION ALL
  SELECT # fallback: just use all data
    id,
    source,
    target,
    pageviews,
    trunc_date
  FROM all_checkpoints WHERE @checkpoint = '-'
),

grouped_checkpoints AS (
  SELECT
    id,
    ANY_VALUE(pageviews) AS pageviews,
    ANY_VALUE(url) AS url,
    ANY_VALUE(trunc_date) AS trunc_date
  FROM picked_checkpoints
  GROUP BY id
)

SELECT
  EXTRACT(YEAR FROM trunc_date) AS year,
  EXTRACT(MONTH FROM trunc_date) AS month,
  EXTRACT(DAY FROM trunc_date) AS day,
  STRING(trunc_date) AS time, -- noqa: RF04
  COUNT(DISTINCT url) AS url,
  SUM(pageviews) AS pageviews
FROM grouped_checkpoints
GROUP BY trunc_date
ORDER BY trunc_date DESC
--- year: the year of the beginning of the reporting interval
--- month: the month of the beginning of the reporting interval
--- day: the day of the beginning of the reporting interval
--- time: the timestamp of the beginning of the reporting interval
--- url: the number of unique URLs in the reporting interval
--- pageviews: the number of page views in the reporting interval that match the criteria
