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
    time,
    TIMESTAMP_TRUNC(time, DAY, @timezone) AS day
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
    day,
    ARRAY_AGG(DISTINCT checkpoint IGNORE NULLS) AS checkpoint
  FROM all_raw_events
  GROUP BY id, hostname, day
),

-- filter billable PageViews
pageviews AS (
  SELECT DISTINCT
    all_raw_events.id,
    'html' AS contenttype,
    all_raw_events.weight,
    all_raw_events.url,
    all_raw_events.hostname,
    all_raw_events.day,
    -- 1 PageView = 1 ContentRequest
    all_raw_events.weight AS contentrequests
  FROM group_all_events_daily
  LEFT JOIN all_raw_events
    ON
      group_all_events_daily.id = all_raw_events.id
      AND group_all_events_daily.hostname = all_raw_events.hostname
      AND group_all_events_daily.day = all_raw_events.day
  -- Content Request = any ID that does not have a checkpoint=404 gets counted as PageView
  WHERE '404' NOT IN UNNEST(group_all_events_daily.checkpoint)
),

-- filter billable API Calls
apicalls AS (
  SELECT
    id,
    'json' AS contenttype,
    weight,
    url,
    hostname,
    day,
    -- 5 APICalls = 1 PageView = 1 ContentRequest
    SUM(weight) * 0.2 AS contentrequests
  FROM all_raw_events
  -- Content Request = any loadresource event that has a target that does not end in .json
  WHERE (checkpoint = 'loadresource' AND target NOT LIKE '%.json')
  GROUP BY id, url, hostname, weight, day
),

dailydata AS (
  SELECT * FROM pageviews
  UNION ALL
  SELECT * FROM apicalls
),

all_data AS (
  # Combine all the data, so that we have it according to desired granularity
  SELECT * FROM dailydata
),

time_series AS (
  SELECT
    hostname,
    url,
    contenttype,
    weight,
    day AS day,
    EXTRACT(YEAR FROM trunc_date) AS year,
    EXTRACT(MONTH FROM trunc_date) AS month,
    SUM(contentrequests) AS contentrequests
  FROM all_data
  GROUP BY hostname, url, contenttype, weight, day
)

SELECT
  year,
  month,
  day,
  hostname,
  url,
  contenttype,
  weight,
  contentrequests
FROM time_series
ORDER BY day DESC
--- year: the year of the beginning of the reporting interval
--- month: the month of the beginning of the reporting interval
--- day: the day of the beginning of the reporting interval
--- hostname: the domain itself
--- url: the URL of the request
--- contenttype: 'html' in case Content Request is counted as pageview, 'json' in case Content Request is of type json api call
--- weight: the sampling weight
--- contentrequests: the number of Content Requests in the reporting interval that match the criteria
