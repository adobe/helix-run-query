--- description: Get ContentRequests, see documentation in https://experienceleague.adobe.com/en/docs/experience-manager-cloud-service/content/implementing/using-cloud-manager/content-requests#cliendside-collection.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- startdate: 2024-01-01
--- enddate: 2024-03-31
--- url: -
--- granularity: 30
--- after: -
--- limit: 1000
--- domainkey: secret
WITH all_raw_events AS (
  SELECT
    id, -- noqa: RF01
    checkpoint, -- noqa: RF01
    source, -- noqa: RF01
    target, -- noqa: RF01
    weight, -- noqa: RF01
    url, -- noqa: RF01
    host, -- noqa: RF01
    hostname, -- noqa: RF01
    user_agent, -- noqa: RF01
    time, -- noqa: RF01
    TIMESTAMP_TRUNC(time, DAY, 'UTC') AS trunc_time # events will be evaluated on day level  -- noqa: RF01
  FROM
    helix_rum.EVENTS_V5(
      @url, # list of urls
      -1, # offset
      -1, # days to fetch
      @startdate, # start date
      @enddate, # end date
      'UTC', # timezone
      'all', # deviceclass
      @domainkey # domain key to prevent data sharing
    )
  WHERE
    hostname != '' -- noqa: RF01
    AND NOT REGEXP_CONTAINS(hostname, r'^\d+\.\d+\.\d+\.\d+$') -- IP addresses -- noqa: RF01
    AND hostname NOT LIKE 'localhost%' -- noqa: RF01
    AND hostname NOT LIKE '%.hlx.page' -- noqa: RF01
    AND hostname NOT LIKE '%.hlx3.page' -- noqa: RF01
    AND hostname NOT LIKE '%.hlx.live' -- noqa: RF01
    AND hostname NOT LIKE '%.helix3.dev' -- noqa: RF01
    AND hostname NOT LIKE '%.sharepoint.com' -- noqa: RF01
    AND hostname NOT LIKE '%.aem.page' -- noqa: RF01
    AND hostname NOT LIKE '%.aem.live' -- noqa: RF01
),

top_hosts AS (
  SELECT
    hostname,
    APPROX_TOP_COUNT(host, 1)[OFFSET(0)].value AS top_host
  FROM all_raw_events
  GROUP BY hostname
),

# IDs can repeat, so we group by hostname and day
group_all_events_daily AS (
  SELECT
    events.id,
    th.top_host,
    events.hostname,
    events.trunc_time,
    events.weight,
    # an ID marks a single HTML request
    events.weight AS html_requests,
    # a JSON request
    COALESCE(
      COUNTIF(
        events.checkpoint = 'loadresource'
        AND events.source NOT LIKE '%.html'
      ),
      0
    )
    * events.weight AS json_requests,
    # request by bot
    COALESCE(COUNTIF(events.user_agent LIKE 'bot%') > 0, false)
      AS is_bot_request,
    # request with 404 error
    COALESCE((COUNTIF(events.checkpoint = '404') > 0), false) AS is_404_request,
    # request to excluded url (html requests)
    COALESCE(
      (
        COUNTIF(
          events.url LIKE '%/manifest.json'
          OR events.url LIKE '%/libs/%'
        )
        > 0
      ),
      false
    ) AS is_excluded_url_request,
    # request to excluded source (json requests)
    COALESCE(
      (
        COUNTIF(
          events.source LIKE '%/api/qraphql/%'
          OR events.source LIKE '%/libs/%'
          OR events.source LIKE '%/manifest.json%'
        )
        > 0
      ), false
    ) AS is_excluded_source_request
  FROM all_raw_events AS events
  LEFT JOIN top_hosts AS th ON events.hostname = th.hostname
  GROUP BY
    events.id, events.hostname, th.top_host, events.trunc_time, events.weight
),

# filter requests
dailydata AS (
  SELECT
    hostname,
    top_host,
    trunc_time,
    weight,
    # PageViews
    # 1 PageView = 1 ContentRequest
    SUM(
      CASE
        WHEN
          is_bot_request IS true
          OR is_404_request IS true
          OR is_excluded_url_request IS true
          THEN 0
        ELSE html_requests
      END
    ) AS cr_pageviews,
    # APICalls
    # 5 APICalls = 1 PageView = 1 ContentRequest
    CAST(SUM(
      CASE
        WHEN is_bot_request IS true OR is_excluded_source_request IS true THEN 0
        ELSE (json_requests / 5)
      END
    ) AS INT64) AS cr_apicalls,
    # HTML requests: Total HTML requests
    SUM(html_requests) AS html_requests,
    # JSON requests: Total JSON requests
    SUM(json_requests) AS json_requests,
    # 404 requests
    CASE
      WHEN is_404_request IS true THEN SUM(html_requests)
      ELSE 0
    END AS error404_requests,
    COUNT(DISTINCT id) AS count_requests
  FROM group_all_events_daily
  GROUP BY
    hostname,
    top_host,
    trunc_time,
    weight,
    is_bot_request,
    is_404_request,
    is_excluded_url_request,
    is_excluded_source_request
),

prepare_monthlydata AS (
  SELECT
    hostname,
    top_host,
    TIMESTAMP_TRUNC(trunc_time, MONTH) AS trunc_time,
    SUM(cr_pageviews + cr_apicalls) AS content_requests,
    SUM(cr_pageviews) AS cr_pageviews,
    SUM(cr_apicalls) AS cr_apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests,
    APPROX_TOP_COUNT(weight, 1)[OFFSET(0)].value AS sampling_rate,
    SUM(count_requests) AS successes
  FROM dailydata
  GROUP BY
    hostname, top_host, TIMESTAMP_TRUNC(trunc_time, MONTH)
),

monthlydata AS (
  SELECT
    hostname,
    top_host,
    trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(CAST(helix_rum.MARGIN_OF_ERROR(
      # sampling rate
      sampling_rate,
      # successes 
      successes,
      # z-score
      CAST(CASE
        # no sampling, confidence level = 100%
        WHEN sampling_rate = 1 THEN 0
        # otherwise 95% confidence level, industry standard
        ELSE 1.96
      END AS NUMERIC)
    ) AS INT64)) AS cr_margin_of_error,
    SUM(cr_pageviews) AS cr_pageviews,
    SUM(cr_apicalls) AS cr_apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests
  FROM prepare_monthlydata
  GROUP BY
    hostname, top_host, trunc_time
  ORDER BY hostname ASC, top_host ASC, trunc_time ASC
),

yearlydata AS (
  SELECT
    hostname,
    top_host,
    TIMESTAMP_TRUNC(trunc_time, YEAR) AS trunc_time,
    SUM(content_requests) AS content_requests,
    SUM(cr_margin_of_error) AS cr_margin_of_error,
    SUM(cr_pageviews) AS cr_pageviews,
    SUM(cr_apicalls) AS cr_apicalls,
    SUM(html_requests) AS html_requests,
    SUM(json_requests) AS json_requests,
    SUM(error404_requests) AS error404_requests
  FROM monthlydata
  GROUP BY
    hostname, top_host, TIMESTAMP_TRUNC(trunc_time, YEAR)
),

alldata_granularity AS (
  SELECT
    TO_HEX(
      SHA1(CONCAT(hostname, '-m-', CAST(UNIX_MICROS(trunc_time) AS STRING)))
    )
      AS row_id,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(trunc_time, YEAR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(trunc_time, MONTH)) AS month,
    *
  FROM monthlydata
  WHERE CAST(@granularity AS INT64) = 30
  UNION ALL
  SELECT
    TO_HEX(
      SHA1(CONCAT(hostname, '-y-', CAST(UNIX_MICROS(trunc_time) AS STRING)))
    )
      AS row_id,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(trunc_time, YEAR)) AS year,
    null AS month,
    *
  FROM yearlydata
  WHERE CAST(@granularity AS INT64) = 365
),

alldata AS (
  SELECT
    row_id,
    trunc_time,
    hostname,
    top_host,
    year,
    month,
    CAST(content_requests AS INT64) AS content_requests,
    CAST(cr_margin_of_error AS INT64) AS cr_margin_of_error,
    CAST(cr_pageviews AS INT64) AS cr_pageviews,
    CAST(cr_apicalls AS INT64) AS cr_apicalls,
    CAST(html_requests AS INT64) AS html_requests,
    CAST(json_requests AS INT64) AS json_requests,
    CAST(error404_requests AS INT64) AS error404_requests,
    # row number
    ROW_NUMBER()
      OVER (
        ORDER BY hostname ASC, trunc_time ASC
      )
      AS rownum,
    row_id = CAST(@after AS STRING) AS is_cursor
  FROM alldata_granularity
  ORDER BY hostname ASC, trunc_time ASC, row_id ASC
),

cursor_rows AS (
  SELECT MIN(rownum) AS rownum FROM alldata
  WHERE is_cursor IS true
  UNION ALL
  SELECT 0 AS rownum FROM alldata
  WHERE @after = '-'
),

cursor_rownum AS (
  SELECT MIN(rownum) AS rownum FROM cursor_rows
)

SELECT
  row_id AS id,
  year,
  month,
  hostname, -- noqa: RF04
  top_host,
  content_requests,
  cr_margin_of_error,
  cr_pageviews,
  cr_apicalls,
  html_requests,
  json_requests,
  error404_requests,
  FORMAT_TIMESTAMP('%Y-%m-%dT%X%Ez', trunc_time) AS time -- noqa: RF04
FROM alldata
WHERE
  (rownum > (SELECT rownum FROM cursor_rownum)) -- noqa: RF02
  AND (rownum <= ((SELECT rownum FROM cursor_rownum) + @limit)) -- noqa: RF02
ORDER BY
  rownum ASC
-- id: the cursor id
-- year: the year of the beginning of the reporting interval
-- month: the month of the beginning of the reporting interval
-- hostname: the domain itself
-- top_host: the most common host for this hostname
-- content_requests: the number of Content Requests in the reporting interval that match the criteria
-- cr_margin_of_error: the margin of error for Content Request
-- cr_pageviews: the number of PageViews
-- cr_apicalls: the number of APICalls
-- html_requests: the total number of HTML Requests
-- json_requests: the total number of JSON Requests
-- error404_requests: the total number of requests returning 404 error
-- time: the timestamp of the beginning of the reporting interval
