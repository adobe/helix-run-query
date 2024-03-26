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
--- url: -
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
WITH hourlydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, HOUR, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
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

dailydata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, DAY, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
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
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, ISOWEEK, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
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
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, MONTH, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
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
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, QUARTER, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
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
    weight AS pageviews,
    url,
    TIMESTAMP_TRUNC(time, YEAR, @timezone) AS trunc_date
  FROM helix_rum.EVENTS_V5(
    @url, # url
    CAST(@offset AS INT64) * 365, # offset in years
    CAST(@interval AS INT64) * 365, # years to fetch
    @startdate, # start date
    @enddate, # end date
    @timezone, # timezone
    'all', # deviceclass
    @domainkey # domain key to prevent data sharing
  )
),

all_checkpoints AS (
  # Combine all the data, so that we have it according to desired granularity
  SELECT * FROM hourlydata WHERE CAST(@granularity AS INT64) = 24
  UNION ALL
  SELECT * FROM dailydata WHERE CAST(@granularity AS INT64) = 1
  UNION ALL
  SELECT * FROM weeklydata WHERE CAST(@granularity AS INT64) = 7
  UNION ALL
  SELECT * FROM monthlydata WHERE CAST(@granularity AS INT64) = 30
  UNION ALL
  SELECT * FROM quarterlydata WHERE CAST(@granularity AS INT64) = 90
  UNION ALL
  SELECT * FROM yearlydata WHERE CAST(@granularity AS INT64) = 365
),

source_target_picked_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(all_checkpoints.pageviews) AS pageviews,
    ANY_VALUE(url) AS url,
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
    ANY_VALUE(url) AS url,
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
    ANY_VALUE(url) AS url,
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
    ANY_VALUE(url) AS url,
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
    url,
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
),

time_series AS (
  SELECT
    trunc_date,
    EXTRACT(YEAR FROM trunc_date AT TIME ZONE @timezone) AS year,
    EXTRACT(MONTH FROM trunc_date AT TIME ZONE @timezone) AS month,
    EXTRACT(DAY FROM trunc_date AT TIME ZONE @timezone) AS day,
    EXTRACT(HOUR FROM trunc_date AT TIME ZONE @timezone) AS hour, -- noqa: RF04
    STRING(trunc_date, @timezone) AS time, -- noqa: RF04
    COUNT(DISTINCT url) AS url,
    SUM(pageviews) AS pageviews
  FROM grouped_checkpoints
  GROUP BY trunc_date
  ORDER BY trunc_date DESC
),

hourlyslots AS (
  # create 24 slots per day for hourly granularity
  SELECT *
  FROM UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP(@startdate, @timezone),
      TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
      INTERVAL 1 HOUR
    )
  ) AS slot
),

dailyslots AS (
  SELECT *
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      -- do not add timezone here because the placeholder 0s may shift a day too early
      DATE(TIMESTAMP_TRUNC(@startdate, DAY)),
      DATE(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
          DAY,
          @timezone
        )
      ),
      INTERVAL 1 DAY
    )
  ) AS slot
),

weeklyslots AS (
  SELECT *
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(TIMESTAMP_TRUNC(@startdate, ISOWEEK, @timezone)),
      DATE(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
          ISOWEEK,
          @timezone
        )
      ),
      INTERVAL 1 WEEK
    )
  ) AS slot
),

monthlyslots AS (
  SELECT *
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(TIMESTAMP_TRUNC(@startdate, MONTH, @timezone)),
      DATE(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
          MONTH,
          @timezone
        )
      ),
      INTERVAL 1 MONTH
    )
  ) AS slot
),

quarterlyslots AS (
  SELECT *
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(TIMESTAMP_TRUNC(@startdate, QUARTER, @timezone)),
      DATE(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
          QUARTER,
          @timezone
        )
      ),
      INTERVAL 1 QUARTER
    )
  ) AS slot
),

yearlyslots AS (
  SELECT *
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(TIMESTAMP_TRUNC(@startdate, YEAR, @timezone)),
      DATE(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(TIMESTAMP(@enddate, @timezone), INTERVAL 23 HOUR),
          YEAR,
          @timezone
        )
      ),
      INTERVAL 1 YEAR
    )
  ) AS slot
),

placeholders AS (
  # placeholders in case some slots have no data
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    24 AS granularity,
    EXTRACT(YEAR FROM slot AT TIME ZONE @timezone) AS year,
    EXTRACT(MONTH FROM slot AT TIME ZONE @timezone) AS month,
    EXTRACT(DAY FROM slot AT TIME ZONE @timezone) AS day,
    EXTRACT(HOUR FROM slot AT TIME ZONE @timezone) AS hour, -- noqa: RF04
    STRING(slot, @timezone) AS time -- noqa: RF04
  FROM hourlyslots
  UNION ALL
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    1 AS granularity,
    EXTRACT(YEAR FROM slot) AS year,
    EXTRACT(MONTH FROM slot) AS month,
    EXTRACT(DAY FROM slot) AS day,
    0 AS hour, -- noqa: RF04
    STRING(TIMESTAMP(slot, @timezone), @timezone) AS time -- noqa: RF04
  FROM dailyslots
  UNION ALL
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    7 AS granularity,
    EXTRACT(YEAR FROM slot) AS year,
    EXTRACT(MONTH FROM slot) AS month,
    EXTRACT(DAY FROM slot) AS day,
    0 AS hour, -- noqa: RF04
    STRING(TIMESTAMP(slot, @timezone), @timezone) AS time -- noqa: RF04
  FROM weeklyslots
  UNION ALL
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    30 AS granularity,
    EXTRACT(YEAR FROM slot) AS year,
    EXTRACT(MONTH FROM slot) AS month,
    EXTRACT(DAY FROM slot) AS day,
    0 AS hour, -- noqa: RF04
    STRING(TIMESTAMP(slot, @timezone), @timezone) AS time -- noqa: RF04
  FROM monthlyslots
  UNION ALL
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    90 AS granularity,
    EXTRACT(YEAR FROM slot) AS year,
    EXTRACT(MONTH FROM slot) AS month,
    EXTRACT(DAY FROM slot) AS day,
    0 AS hour, -- noqa: RF04
    STRING(TIMESTAMP(slot, @timezone), @timezone) AS time -- noqa: RF04
  FROM quarterlyslots
  UNION ALL
  SELECT
    0 AS url,
    0 AS pageviews,
    0 AS pageviews_forecast,
    0 AS url_forecast,
    365 AS granularity,
    EXTRACT(YEAR FROM slot) AS year,
    EXTRACT(MONTH FROM slot) AS month,
    EXTRACT(DAY FROM slot) AS day,
    0 AS hour, -- noqa: RF04
    STRING(TIMESTAMP(slot, @timezone), @timezone) AS time -- noqa: RF04
  FROM yearlyslots
),

allslots AS (
  SELECT
    year,
    month,
    day,
    hour, -- noqa: RF04
    time, -- noqa: RF04
    url,
    pageviews,
    IF(
      # this is the first row
      ROW_NUMBER() OVER (ORDER BY trunc_date DESC) = 1,
      CAST((
        # apply rule of three to calculate the progress of the current interval
        # multiplied by the pageviews
        (
          pageviews
          / GREATEST(
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trunc_date, HOUR)
            / (24 * CAST('24' AS INT64)), 1
          )
        )
        * 0.5
        # 50% weight for the progress of the current interval
      )
      +
      (
      # moving average of the last 7 items
        AVG(pageviews)
          OVER (
            ORDER BY trunc_date ASC
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
          )
        * 0.5
        # 50% weight for the moving average
      ) AS INT64)
      ,
      pageviews
    ) AS pageviews_forecast,
    IF(
      # this is the first row
      ROW_NUMBER() OVER (ORDER BY trunc_date DESC) = 1,
      CAST((
        # apply rule of three to calculate the progress of the current interval
        # multiplied with the pageviews
        (
          url
          / GREATEST(
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trunc_date, HOUR)
            / (24 * CAST('24' AS INT64)), 1
          )
        )
        * 0.2
        # 20% weight for the progress of the current interval
      )
      +
      (
      # moving average of the last 7 items
        AVG(url)
          OVER (
            ORDER BY trunc_date ASC
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
          )
        * 0.8
        # 80% weight for the moving average
        # Most of the time, the number of unique URLs is not changing that much
        # over the course of a day/week, so we give historical data a higher weight
      ) AS INT64)
      ,
      url
    ) AS url_forecast
  FROM time_series
  # combine data results with placeholders
  UNION DISTINCT
  SELECT
    year,
    month,
    day,
    hour,
    time,
    url,
    pageviews,
    pageviews_forecast,
    url_forecast
  FROM placeholders
  WHERE CAST(@granularity AS INT64) = granularity
)

SELECT
  year,
  month,
  day,
  hour,
  time,
  SUM(url) AS url,
  SUM(pageviews) AS pageviews,
  SUM(pageviews_forecast) AS pageviews_forecast,
  SUM(url_forecast) AS url_forecast
FROM allslots
GROUP BY year, month, day, hour, time
ORDER BY time DESC
--- year: the year of the beginning of the reporting interval
--- month: the month of the beginning of the reporting interval
--- day: the day of the beginning of the reporting interval
--- hour: the hour of the beginning of the reporting interval
--- time: the timestamp of the beginning of the reporting interval
--- url: the number of unique URLs in the reporting interval
--- pageviews: the number of page views in the reporting interval that match the criteria
--- pageviews_forecast: the forecasted number of page views in the reporting interval, based on the last 7 full data points
--- url_forecast: the forecasted number of unique URLs in the reporting interval, based on the last 7 full data points
