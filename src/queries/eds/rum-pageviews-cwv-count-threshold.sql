--- description: Get pageviews for a given URL or domain having a Core Web Vital events count above a given threshold
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- timezone: UTC
--- device: all
--- url: -
--- cwv_type: lcp
--- cwv_count_threshold: 100
--- avg_daily_pageviews_factor: 1000
--- domainkey: secret


WITH current_data AS (
  SELECT * FROM
    helix_rum.EVENTS_V3(
      @url, -- domain or URL
      CAST(@offset AS INT64), -- not used, offset in days from today
      CAST(@interval AS INT64), -- interval in days to consider
      @startdate, -- not used, start date
      @enddate, -- not used, end date
      @timezone, -- timezone
      @device, -- device class
      @domainkey
    )
),

current_rum_by_id AS (
  SELECT
    id,
    ANY_VALUE(host) AS host,
    ANY_VALUE(user_agent) AS user_agent,
    ANY_VALUE(url) AS url,
    MAX(CASE
      WHEN @cwv_type = "lcp" THEN lcp
      WHEN @cwv_type = "cls" THEN cls
      WHEN @cwv_type = "fid" THEN fid
      WHEN @cwv_type = "inp" THEN inp
    END) AS core_web_vital,
    ANY_VALUE(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  WHERE
    url LIKE CONCAT("https://", @url, "%")
  GROUP BY id
),

current_events_by_url AS (
  SELECT
    url,
    COUNT(id) AS events
  FROM current_rum_by_id
  GROUP BY url
  ORDER BY events DESC
),

current_rum_by_url_and_weight AS (
  SELECT
    url,
    MAX(weight) AS weight,
    CAST(APPROX_QUANTILES(core_web_vital, 100)[OFFSET(75)] AS INT64)
      AS avg_core_web_vital
  FROM current_rum_by_id
  GROUP BY url
),

url_above_cwv_count_threshold AS (
  SELECT
    filtered_data.url,
    filtered_data.cwv_count,
    cru.avg_core_web_vital AS avg_cwv,
    (ce.events * cru.weight) AS pageviews
  FROM (
    SELECT
      cr.url,
      @cwv_type as cwv_type,
      COUNT(*) AS cwv_count
    FROM current_rum_by_id AS cr
    WHERE core_web_vital IS NOT NULL
    GROUP BY url
  ) AS filtered_data
  LEFT JOIN
    current_events_by_url AS ce
    ON filtered_data.url = ce.url
  LEFT JOIN
    current_rum_by_url_and_weight AS cru
    ON filtered_data.url = cru.url
  WHERE
    cwv_count > @cwv_count_threshold
    AND (ce.events * cru.weight) > @interval * @avg_daily_pageviews_factor
)

SELECT
  url,
  cw_type,
  cwv_count,
  pageviews,
  avg_cwv
FROM url_above_cwv_count_threshold;
