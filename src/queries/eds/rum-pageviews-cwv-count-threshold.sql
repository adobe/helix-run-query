--- description: Get pageviews for a given URL or domain having a Core Web Vital events count above a given threshold
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- timezone: UTC
--- device: all
--- url: -
--- owner: -
--- repo: -
--- cwv_type: lcp
--- cwv_count_threshold: 100
--- sampling_noise_factor: 1000
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
) 
current_rum_by_id AS (
  SELECT
    id,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    MAX(url) as url,
    MAX(CASE WHEN @cwv_type = "lcp" THEN lcp
        WHEN @cwv_type = "cls" THEN cls
        WHEN @cwv_type = "fid" THEN fid
        WHEN @cwv_type = "inp" THEN inp
    ELSE NULL END) AS core_web_vital,
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  WHERE
    url LIKE CONCAT("https://", @url, "%")
     OR url LIKE CONCAT(
      "https://%", @repo, "--", @owner, ".hlx%/"
    ) OR (@url = "-" AND @repo = "-" AND @owner = "-")
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
    MAX(weight) AS weight,
    url,
    CAST(APPROX_QUANTILES(core_web_vital, 100)[OFFSET(75)] AS INT64) AS avg_core_web_vital
  FROM current_rum_by_id
  GROUP BY url
),
url_above_cwv_count_threshold AS (
SELECT
  filtered_data.url,
  cwv_count,
  (ce.events * cru.weight) as pageviews,
  cru.avg_core_web_vital as avg_cwv
FROM (
  SELECT
    cr.url,
    @cwv_type,
    count(*) as cwv_count
    from current_rum_by_id cr
  WHERE core_web_vital is not null
  GROUP BY url
) filtered_data
LEFT JOIN
  current_events_by_url ce ON ce.url = filtered_data.url
LEFT JOIN
  current_rum_by_url_and_weight cru ON cru.url = filtered_data.url
WHERE cwv_count > @cwv_count_threshold
 AND (ce.events * cru.weight) > @interval * @sampling_noise_factor
)
SELECT 
  url,
  cw_type,
  cwv_count,
  pageviews,
  avg_cwv
FROM url_above_cwv_count_threshold;