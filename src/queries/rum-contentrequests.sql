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

-- filter PageViews: HTML requests
pageviews AS (
  SELECT DISTINCT
    all_raw_events.id,
    all_raw_events.weight,
    all_raw_events.url,
    all_raw_events.hostname,
    all_raw_events.trunc_time,
    all_raw_events.weight AS requests
  FROM group_all_events_daily
  LEFT JOIN all_raw_events
    ON
      group_all_events_daily.id = all_raw_events.id
      AND group_all_events_daily.hostname = all_raw_events.hostname
      AND group_all_events_daily.trunc_time = all_raw_events.trunc_time
  -- PageView = any ID that does not have a checkpoint=404
  WHERE '404' NOT IN UNNEST(group_all_events_daily.checkpoint)
  -- bots are excluded
  AND all_raw_events.user_agent != 'bot'
),

-- filter API Calls: JSON requests
apicalls AS (
  SELECT
    id,
    hostname,
    url,
    weight,
    user_agent,
    trunc_time,
    SUM(weight) AS requests
  FROM all_raw_events
  -- APICall = any loadresource event that has a target that does not end in .json
  WHERE (checkpoint = 'loadresource' AND target NOT LIKE '%.json')
  -- bots are excluded
  AND user_agent != 'bot'
  GROUP BY id, url, hostname, weight, user_agent, trunc_time
),

-- filter 404 requests
error404 AS (
  SELECT DISTINCT
    all_raw_events.id,
    all_raw_events.weight,
    all_raw_events.url,
    all_raw_events.hostname,
    all_raw_events.trunc_time,
    all_raw_events.weight AS requests
  FROM group_all_events_daily
  LEFT JOIN all_raw_events
    ON
      group_all_events_daily.id = all_raw_events.id
      AND group_all_events_daily.hostname = all_raw_events.hostname
      AND group_all_events_daily.trunc_time = all_raw_events.trunc_time
  WHERE '404' IN UNNEST(group_all_events_daily.checkpoint)
),

-- filter bot requests
bots AS (
  SELECT
    id,
    hostname,
    url,
    weight,
    user_agent,
    trunc_time,
    SUM(weight) AS requests
  FROM all_raw_events
  WHERE user_agent = 'bot'
  GROUP BY id, url, hostname, weight, user_agent, trunc_time
),

group_all_events_with_details_daily AS (
  SELECT
    id,
    hostname,
    url,
    weight,
    user_agent,
    trunc_time
  FROM all_raw_events
  GROUP BY id, hostname, url, weight, user_agent, trunc_time
),

-- filter requests
dailydata AS (
  SELECT
    e.hostname,
    e.url,
    e.weight,
    e.user_agent,
    e.trunc_time,
    -- HTML requests: PageViews
    SUM(COALESCE(pv.requests, 0)) AS html_requests,
    -- JSON requests: APICalls
    SUM(COALESCE(ac.requests, 0)) AS json_requests,
    -- bot requests
    SUM(COALESCE(b.requests, 0)) AS bot_requests,
    -- 404 requests
    SUM(COALESCE(er.requests, 0)) AS error404_requests,
    -- 1 PageView = 1 ContentRequest
    -- 5 APICalls = 1 PageView = 1 ContentRequest
    SUM(COALESCE(pv.requests, 0))
    + SUM(COALESCE(ac.requests, 0) * 0.2) AS content_requests
  FROM group_all_events_with_details_daily AS e
  LEFT JOIN pageviews AS pv
    ON
      e.id = pv.id
      AND e.hostname = pv.hostname
      AND e.url = pv.url
      AND e.weight = pv.weight
      AND e.trunc_time = pv.trunc_time
  LEFT JOIN apicalls AS ac
    ON
      e.id = ac.id
      AND e.hostname = ac.hostname
      AND e.url = ac.url
      AND e.weight = ac.weight
      AND e.trunc_time = ac.trunc_time
  LEFT JOIN error404 AS er
    ON
      e.id = er.id
      AND e.hostname = er.hostname
      AND e.url = er.url
      AND e.weight = er.weight
      AND e.trunc_time = er.trunc_time
  LEFT JOIN bots AS b
    ON
      e.id = b.id
      AND e.hostname = b.hostname
      AND e.url = b.url
      AND e.weight = b.weight
      AND e.trunc_time = b.trunc_time
  GROUP BY e.hostname, e.url, e.weight, e.user_agent, e.trunc_time
),

monthlydata AS (
  SELECT
    hostname,
    url,
    weight,
    user_agent,
    TIMESTAMP_TRUNC(trunc_time, MONTH) AS trunc_time,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(bot_requests) AS bot_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(content_requests) AS content_requests
  FROM dailydata
  GROUP BY
    hostname, url, weight, user_agent,
    TIMESTAMP_TRUNC(trunc_time, MONTH)
),

yearlydata AS (
  SELECT
    hostname,
    url,
    weight,
    user_agent,
    TIMESTAMP_TRUNC(trunc_time, YEAR) AS trunc_time,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(bot_requests) AS bot_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(content_requests) AS content_requests
  FROM dailydata
  GROUP BY
    hostname, url, weight, user_agent,
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
    url,
    weight,
    user_agent,
    html_requests,
    json_requests,
    bot_requests,
    error404_requests,
    content_requests,
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
    'all' AS url,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT user_agent IGNORE NULLS),
      ', '
    ) AS user_agent,
    AVG(weight) AS weight,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(bot_requests) AS bot_requests,
    SUM(error404_requests) AS error404_requests,
    SUM(content_requests) AS content_requests
  FROM aggregate_default
  GROUP BY year, month, day, trunc_time, hostname
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
    weight,
    user_agent,
    html_requests,
    json_requests,
    bot_requests,
    error404_requests,
    content_requests
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
    weight,
    user_agent,
    html_requests,
    json_requests,
    bot_requests,
    error404_requests,
    content_requests
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
  weight,
  user_agent,
  html_requests,
  json_requests,
  bot_requests,
  error404_requests,
  content_requests
FROM aggregate_result
-- year: the year of the beginning of the reporting interval
-- month: the month of the beginning of the reporting interval
-- day: the day of the beginning of the reporting interval
-- trunc_time: the timestamp of the beginning of the reporting interval
-- hostname: the domain itself
-- url: the URL of the request ('all' in 'reduce' result)
-- weight: the sampling weight (average weight in 'reduce' result)
-- user_agent: the user agent of the requests
-- html_requests: the number of PageViews
-- json_requests: the number of APICalls
-- bot_requests: the number of requests by bots
-- error404_requests: the number of requests returning 404 error
-- content_requests: the number of Content Requests in the reporting interval that match the criteria
