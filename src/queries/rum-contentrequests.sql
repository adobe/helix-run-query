--- description: Get ContentRequests.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2023-01-01
--- enddate: 2023-12-31
--- url: -
--- granularity: 30
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
      COUNTIF(
        checkpoint = 'loadresource'
        AND target NOT LIKE '%.json'
      ),
      0
    )
    * weight AS json_requests,
    # request by bot
    COALESCE(COUNTIF(user_agent = 'bot') > 1, false) AS is_bot_request,
    # request with 404 error
    COALESCE((COUNTIF(checkpoint = '404') > 0), false) AS is_404_request
  FROM all_raw_events
  GROUP BY id, hostname, trunc_time, weight
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
    END AS error404_requests
  FROM group_all_events_daily
  GROUP BY hostname, trunc_time, weight, is_bot_request, is_404_request
),

monthlydata AS (
  SELECT
    hostname,
    TIMESTAMP_TRUNC(trunc_time, MONTH) AS trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(pageviews) AS pageviews,
    SUM(apicalls) AS apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    # content requests is sampled data
    # margin of error for content requests
    helix_rum.CALC_BINOMIAL_DISTRIBUTION_MARGIN_OF_ERROR_TEMP(
      # sampling rate
      MAX(weight),
      # successes 
      (SUM(content_requests) * (1 / MAX(weight))),
      # z-score
      CAST(CASE
        # no sampling, confidence level = 100%
        WHEN MAX(weight) = 1 THEN 0
        # otherwise 95% confidence level, industry standard
        ELSE 1.96
      END AS NUMERIC)
    ) AS content_requests_margin_of_error
  FROM dailydata
  GROUP BY
    hostname, TIMESTAMP_TRUNC(trunc_time, MONTH)
),

yearlydata AS (
  SELECT
    hostname,
    TIMESTAMP_TRUNC(trunc_time, YEAR) AS trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(pageviews) AS pageviews,
    SUM(apicalls) AS apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    # content requests is sampled data
    # margin of error for content requests
    helix_rum.CALC_BINOMIAL_DISTRIBUTION_MARGIN_OF_ERROR_TEMP(
      # sampling rate
      MAX(weight),
      # successes 
      (SUM(content_requests) * (1 / MAX(weight))),
      # z-score
      CAST(CASE
        # no sampling, confidence level = 100%
        WHEN MAX(weight) = 1 THEN 0
        # otherwise 95% confidence level, industry standard
        ELSE 1.96
      END AS NUMERIC)
    ) AS content_requests_margin_of_error
  FROM dailydata
  GROUP BY
    hostname, TIMESTAMP_TRUNC(trunc_time, YEAR)
),

alldata_granularity AS (
  --- Desired granularity
  SELECT
    CONCAT(hostname, '-', CAST(UNIX_MICROS(trunc_time) AS STRING)) AS row_id,
    *
  FROM monthlydata WHERE CAST(@granularity AS INT64) = 30
  UNION ALL
  SELECT
    CONCAT(hostname, '-', CAST(UNIX_MICROS(trunc_time) AS STRING)) AS row_id,
    *
  FROM yearlydata WHERE CAST(@granularity AS INT64) = 365
),

alldata AS (
  SELECT
    row_id,
    trunc_time,
    hostname,
    content_requests,
    content_requests_margin_of_error,
    pageviews,
    apicalls,
    html_requests,
    # Lower Bound: Content Requests - Margin of Error
    # Make sure lower bound cannot be < 1
    json_requests,
    # Upper Bound: Content Requests + Margin of Error
    error404_requests,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(trunc_time, YEAR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(trunc_time, MONTH)) AS month,
    EXTRACT(DAY FROM TIMESTAMP_TRUNC(trunc_time, DAY)) AS day,
    CAST(
      GREATEST(
        (content_requests - content_requests_margin_of_error), 1
      ) AS INT64
    )
      AS content_requests_marginal_err_excl,
    CAST((content_requests + content_requests_margin_of_error) AS INT64)
      AS content_requests_marginal_err_incl,
    # row number
    ROW_NUMBER()
      OVER (ORDER BY hostname ASC, trunc_time ASC)
      AS rownum,
    row_id = CAST(@after AS STRING) AS is_cursor
  FROM alldata_granularity
  ORDER BY hostname ASC, trunc_time ASC
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
  content_requests,
  content_requests_marginal_err_excl,
  content_requests_marginal_err_incl,
  pageviews,
  apicalls,
  html_requests,
  json_requests,
  error404_requests,
  rownum,
  FORMAT_TIMESTAMP('%Y-%m-%dT%X%Ez', trunc_time) AS time -- noqa: RF04
FROM alldata
WHERE
  (rownum > (SELECT rownum FROM cursor_rownum))
  AND (rownum <= ((SELECT rownum FROM cursor_rownum) + 1000))
ORDER BY
  rownum ASC
-- id: the cursor id
-- year: the year of the beginning of the reporting interval
-- month: the month of the beginning of the reporting interval
-- day: the day of the beginning of the reporting interval
-- hostname: the domain itself
-- content_requests: the number of Content Requests in the reporting interval that match the criteria
-- pageviews: the number of PageViews
-- apicalls: the number of APICalls
-- html_requests: the total number of HTML Requests
-- json_requests: the total number of JSON Requests
-- error404_requests: the total number of requests returning 404 error
-- rownum: the number of the row
-- time: the timestamp of the beginning of the reporting interval
-- content_requests_marginal_err_excl: the lower bound of Content Requests respecting the sampling error
-- content_requests_marginal_err_incl: the upper bound of Content Requests respecting the sampling error
