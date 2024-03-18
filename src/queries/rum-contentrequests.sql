--- description: Get ContentRequests.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2023-01-01
--- enddate: 2023-12-31
--- url: -
--- granularity: 1
--- aggregate: - (default) or 'reduce'
--- timezone: UTC
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
    ARRAY_AGG(DISTINCT checkpoint IGNORE NULLS) AS checkpoint
  FROM all_raw_events
  GROUP BY id, hostname, trunc_time
),

-- filter billable PageViews
pageviews AS (
  SELECT DISTINCT
    all_raw_events.id,
    'html' AS content_type,
    all_raw_events.weight,
    all_raw_events.url,
    all_raw_events.hostname,
    all_raw_events.trunc_time,
    -- 1 PageView = 1 ContentRequest
    all_raw_events.weight AS content_requests,
    -- keep the number of all requests
    all_raw_events.weight AS count_requests
  FROM group_all_events_daily
  LEFT JOIN all_raw_events
    ON
      group_all_events_daily.id = all_raw_events.id
      AND group_all_events_daily.hostname = all_raw_events.hostname
      AND group_all_events_daily.trunc_time = all_raw_events.trunc_time
  -- Content Request = any ID that does not have a checkpoint=404 gets counted as PageView
  WHERE '404' NOT IN UNNEST(group_all_events_daily.checkpoint)
  -- bots are excluded from Content Requests
  AND all_raw_events.user_agent != 'bot'
),

-- filter billable API Calls
apicalls AS (
  SELECT
    id,
    'json' AS content_type,
    weight,
    url,
    hostname,
    trunc_time,
    -- 5 APICalls = 1 PageView = 1 ContentRequest
    SUM(weight) * 0.2 AS content_requests,
    -- keep the number of all requests
    SUM(weight) AS count_requests
  FROM all_raw_events
  -- Content Request = any loadresource event that has a target that does not end in .json
  WHERE (checkpoint = 'loadresource' AND target NOT LIKE '%.json')
  -- bots are excluded from Content Requests
  AND user_agent != 'bot'
  GROUP BY id, url, hostname, weight, trunc_time
),

dailydata AS (
  SELECT
    content_type,
    weight,
    url,
    hostname,
    trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(count_requests) AS count_requests
  FROM pageviews
  GROUP BY content_type, weight, url, hostname, trunc_time
  UNION ALL
  SELECT
    content_type,
    weight,
    url,
    hostname,
    trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(count_requests) AS count_requests
  FROM apicalls
  GROUP BY content_type, weight, url, hostname, trunc_time
),

monthlydata AS (
  SELECT
    content_type,
    weight,
    url,
    hostname,
    TIMESTAMP_TRUNC(trunc_time, MONTH) AS trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(count_requests) AS count_requests
  FROM dailydata
  GROUP BY
    content_type, weight, url, hostname,
    TIMESTAMP_TRUNC(trunc_time, MONTH)
),

yearlydata AS (
  SELECT
    content_type,
    weight,
    url,
    hostname,
    TIMESTAMP_TRUNC(trunc_time, YEAR) AS trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(count_requests) AS count_requests
  FROM dailydata
  GROUP BY
    content_type, weight, url, hostname,
    TIMESTAMP_TRUNC(trunc_time, YEAR)
),

all_data AS (
  # Desired granularity
  SELECT * FROM dailydata WHERE CAST(@granularity AS INT64) = 1
  UNION ALL
  SELECT * FROM monthlydata WHERE CAST(@granularity AS INT64) = 30
  UNION ALL
  SELECT * FROM yearlydata WHERE CAST(@granularity AS INT64) = 365
),

aggregate_default AS (
  SELECT
    trunc_time,
    hostname,
    content_type,
    url,
    weight,
    content_requests,
    count_requests,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(trunc_time, YEAR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(trunc_time, MONTH)) AS month,
    EXTRACT(DAY FROM TIMESTAMP_TRUNC(trunc_time, DAY)) AS day
  FROM all_data
  ORDER BY year DESC, month DESC, day DESC
),

-- reduce to 1 value per hostname, per date, per content type
aggregate_reduce AS (
  SELECT
    year,
    month,
    day,
    trunc_time,
    hostname,
    content_type,
    'all' AS url,
    AVG(weight) AS weight,
    SUM(content_requests) AS content_requests,
    SUM(count_requests) AS count_requests
  FROM aggregate_default
  GROUP BY year, month, day, trunc_time, hostname, content_type
  ORDER BY year DESC, month DESC, day DESC
),

aggregate_result AS (
  SELECT
    year,
    month,
    day,
    trunc_time,
    hostname,
    url,
    content_type,
    weight,
    content_requests,
    count_requests
  FROM aggregate_reduce
  WHERE @aggregate = 'reduce'
  UNION ALL
  SELECT
    year,
    month,
    day,
    trunc_time,
    hostname,
    url,
    content_type,
    weight,
    content_requests,
    count_requests
  FROM aggregate_default
  WHERE @aggregate != 'reduce'
)

SELECT
  year,
  month,
  day,
  trunc_time,
  hostname,
  url,
  content_type,
  weight,
  content_requests,
  count_requests
FROM aggregate_result
-- year: the year of the beginning of the reporting interval
-- month: the month of the beginning of the reporting interval
-- day: the day of the beginning of the reporting interval
-- trunc_time: the timestamp of the beginning of the reporting interval
-- hostname: the domain itself
-- url: the URL of the request ('all' in 'reduce' result)
-- content_type: 'html' in case Content Request is counted as PageView, 'json' in case Content Request is counted as APICall 
-- weight: the sampling weight (average weight in 'reduce' result)
-- content_requests: the number of Content Requests in the reporting interval that match the criteria
-- count_requests: the number of requests which resulted to the number of Content Requests
