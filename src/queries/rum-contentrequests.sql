--- description: Get ContentRequests.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2023-01-01
--- enddate: 2023-12-31
--- url: -
--- granularity: 1
--- timezone: UTC
--- after: -
--- limit: 1000
--- domainkey: secret
WITH all_raw_events AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    weight,
    url,
    hostname,
    user_agent,
    time,
    TIMESTAMP_TRUNC(time, DAY, @timezone) AS trunc_time # events will be evaluated on day level
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

-- IDs can repeat, so we group by hostname and day
group_all_events_daily AS (
  SELECT
    id,
    hostname,
    trunc_time,
    weight,
    # an ID marks a single HTML request
    weight AS html_requests,
    # a JSON request
    COALESCE(
      COUNTIF(checkpoint = 'loadresource' AND target NOT LIKE '%.json'), 0
    )
    * weight AS json_requests,
    # request by bot
    COALESCE(user_agent = 'bot', false) AS is_bot_request,
    # request with 404 error
    COALESCE((COUNTIF(checkpoint = '404') > 0), false) AS is_404_request
  FROM all_raw_events
  GROUP BY id, hostname, trunc_time, weight, user_agent
),

-- filter requests
dailydata AS (
  SELECT
    hostname,
    trunc_time,
    weight,
    # 1 PageView = 1 ContentRequest
    # 5 APICalls = 1 PageView = 1 ContentRequest
    # Exclude Bots and 404
    SUM(
      CASE
        WHEN is_bot_request OR is_404_request THEN 0
        ELSE (html_requests + (json_requests * 0.2))
      END
    ) AS content_requests,
    -- PageViews
    SUM(
      CASE
        WHEN is_bot_request OR is_404_request THEN 0
        ELSE html_requests
      END
    ) AS pageviews,
    -- APICalls
    SUM(
      CASE
        WHEN is_bot_request OR is_404_request THEN 0
        ELSE json_requests
      END
    ) AS apicalls,
    -- HTML requests: Total HTML requests
    SUM(html_requests) AS html_requests,
    -- JSON requests: Total JSON requests
    SUM(json_requests) AS json_requests,
    -- 404 requests
    CASE
      WHEN is_bot_request THEN SUM(html_requests)
      ELSE 0
    END AS error404_requests,
    -- Bot requests
    CASE
      WHEN is_bot_request THEN SUM(html_requests)
      ELSE 0
    END AS bot_html_requests,
    CASE
      WHEN is_bot_request THEN SUM(json_requests)
      ELSE 0
    END AS bot_json_requests
  FROM group_all_events_daily
  GROUP BY hostname, trunc_time, weight, is_bot_request, is_404_request
),

monthlydata AS (
  SELECT
    hostname,
    TIMESTAMP_TRUNC(trunc_time, MONTH) AS trunc_time,
    weight,
    SUM(content_requests) AS content_requests,
    SUM(pageviews) AS pageviews,
    SUM(apicalls) AS apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(bot_html_requests) AS bot_html_requests,
    SUM(bot_json_requests) AS bot_json_requests
  FROM dailydata
  GROUP BY
    hostname, weight,
    TIMESTAMP_TRUNC(trunc_time, MONTH)
),

yearlydata AS (
  SELECT
    hostname,
    TIMESTAMP_TRUNC(trunc_time, YEAR) AS trunc_time,
    weight,
    SUM(content_requests) AS content_requests,
    SUM(pageviews) AS pageviews,
    SUM(apicalls) AS apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(bot_html_requests) AS bot_html_requests,
    SUM(bot_json_requests) AS bot_json_requests
  FROM dailydata
  GROUP BY
    hostname, weight,
    TIMESTAMP_TRUNC(trunc_time, YEAR)
),

alldata_granularity AS (
  --- Desired granularity
  SELECT
    CONCAT(hostname, '-', CAST(weight AS STRING), '-', CAST(UNIX_MICROS(trunc_time) AS STRING)) AS row_id,
    *
  FROM dailydata WHERE CAST(@granularity AS INT64) = 1
  UNION ALL
  SELECT
    CONCAT(hostname, '-', CAST(weight AS STRING), '-', CAST(UNIX_MICROS(trunc_time) AS STRING)) AS row_id,
    *
  FROM monthlydata WHERE CAST(@granularity AS INT64) = 30
  UNION ALL
  SELECT
    CONCAT(hostname, '-', CAST(weight AS STRING), '-', CAST(UNIX_MICROS(trunc_time) AS STRING)) AS row_id,
    *
  FROM yearlydata WHERE CAST(@granularity AS INT64) = 365
),

alldata AS (
  SELECT
    row_id,
    trunc_time,
    hostname,
    weight,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(trunc_time, YEAR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(trunc_time, MONTH)) AS month,
    EXTRACT(DAY FROM TIMESTAMP_TRUNC(trunc_time, DAY)) AS day,
    SUM(pageviews) AS pageviews,
    SUM(apicalls) AS apicalls,
    SUM(content_requests) AS content_requests,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(bot_html_requests) AS bot_html_requests,
    SUM(bot_json_requests) AS bot_json_requests,
    CASE
      # no sampling, confidence level = 100%
      WHEN weight = 1 THEN 100
      # otherwise 95% confidence level, industry standard
      ELSE 95
    END AS confidence_level,
    # get margin of error for sampling
    helix_rum.CALC_BINOMIAL_DISTRIBUTION_MARGIN_OF_ERROR_TEMP(
      # sampling rate
      weight,
      # successes 
      (SUM(content_requests) * (1 / weight)),
      # z-score
      CAST(CASE
        # no sampling, confidence level = 100%
        WHEN weight = 1 THEN 0
        # otherwise 95% confidence level, industry standard
        ELSE 1.96
      END AS NUMERIC)
    ) AS margin_of_error,
    # row number
    ROW_NUMBER()
      OVER (ORDER BY hostname ASC, trunc_time ASC, weight ASC)
      AS rownum,
    row_id = CAST(@after AS STRING) AS is_cursor
  FROM alldata_granularity
  GROUP BY row_id, year, month, day, trunc_time, hostname, weight
  ORDER BY hostname ASC, trunc_time ASC, weight ASC
),

cursor_rows AS (
  SELECT MIN(rownum) AS rownum FROM alldata WHERE is_cursor
  UNION ALL
  SELECT 0 AS rownum FROM alldata WHERE @after = '-'
),

cursor_rownum AS (
  SELECT MIN(rownum) AS rownum FROM cursor_rows
)

SELECT
  row_id AS id,
  year,
  month,
  day,
  hostname, -- noqa: RF04
  weight,
  content_requests,
  confidence_level,
  margin_of_error,
  pageviews,
  # Lower Bound
  # Formula: Content Requests - Margin of Error
  # Make sure lower bound cannot be < 1
  apicalls,
  # Upper Bound
  # Formula: Content Requests + Margin of Error
  html_requests,
  json_requests,
  error404_requests,
  bot_html_requests,
  bot_json_requests,
  rownum,
  FORMAT_TIMESTAMP('%Y-%m-%dT%X%Ez', trunc_time) AS time, -- noqa: RF04
  CAST(GREATEST((content_requests - margin_of_error), 1) AS INT64)
    AS lower_bound,
  CAST((content_requests + margin_of_error) AS INT64) AS upper_bound
FROM alldata
WHERE
  (rownum > (SELECT rownum FROM cursor_rownum))
  AND (rownum <= ((SELECT rownum FROM cursor_rownum) + @limit))
ORDER BY
  rownum ASC
-- id: the cursor id
-- year: the year of the beginning of the reporting interval
-- month: the month of the beginning of the reporting interval
-- day: the day of the beginning of the reporting interval
-- time: the timestamp of the beginning of the reporting interval
-- hostname: the domain itself
-- weight: the sampling weight
-- content_requests: the number of Content Requests in the reporting interval that match the criteria
-- confidence_level: the level of confidence for the Content Requests calculation
-- margin_of_error: the margin of error  for the Content Requests calculation
-- lower_bound: the lower bound of Content Requests respecting the sampling error
-- upper_bound: the upper bound of Content Requests respecting the sampling error
-- pageviews: the number of PageViews
-- apicalls: the number of APICalls
-- html_requests: the total number of HTML Requests
-- json_requests: the total number of JSON Requests
-- error404_requests: the total number of requests returning 404 error
-- bot_html_requests: the total number of HTML requests by bots
-- bot_json_requests: the total number of JSON requests by bots
-- rownum: the number of the row
