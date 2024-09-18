--- description: Get form view, submission, and core web vitals for Forms domains
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- domainkey: secret

WITH
current_checkpoints AS (
  SELECT
    *,
    TIMESTAMP_TRUNC(time, DAY, @timezone) AS date
  FROM
    helix_rum.CHECKPOINTS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
),

view_urls AS (
  SELECT
    url,
    checkpoint,
    source,
    COUNT(id) AS ids,
    COUNT(DISTINCT id) * MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_checkpoints
  WHERE
    checkpoint = 'viewblock'
    AND (source = '.form' OR source = '.marketo' OR source IS NULL)
  GROUP BY url, checkpoint, source
),

submission_urls AS (
  SELECT
    url,
    checkpoint,
    source,
    COUNT(id) AS ids,
    COUNT(DISTINCT id) * MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_checkpoints
  WHERE
    checkpoint = 'formsubmit'
    AND (
      source LIKE '%.form%'
      OR source LIKE '%mktoForm%'
      OR source LIKE '%.marketo%'
    )
  GROUP BY url, checkpoint, source
),

current_data AS (
  SELECT * FROM
    helix_rum.EVENTS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
  WHERE
    weight != 1
),

current_rum_by_id AS (
  SELECT
    id,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    IF(
      @url = '-',
      REGEXP_EXTRACT(MAX(url), 'https://([^/]+)/', 1),
      MAX(url)
    ) AS url,
    MAX(lcp) AS lcp,
    MAX(fid) AS fid,
    MAX(inp) AS inp,
    MAX(cls) AS cls,
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  WHERE
    url LIKE CONCAT(
      'https://', @url, '%'
    )
  GROUP BY id
),

current_rum_by_url_and_weight AS (
  SELECT
    weight,
    url,
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
    CAST(APPROX_QUANTILES(inp, 100)[OFFSET(75)] AS INT64) AS avginp,
    ROUND(APPROX_QUANTILES(cls, 100)[OFFSET(75)], 3) AS avgcls
  FROM current_rum_by_id
  GROUP BY url, weight
),

current_rum_by_url AS (
  SELECT
    url,
    CAST(SUM(avglcp * weight) / SUM(weight) AS INT64) AS avglcp,
    CAST(SUM(avgfid * weight) / SUM(weight) AS INT64) AS avgfid,
    CAST(SUM(avginp * weight) / SUM(weight) AS INT64) AS avginp,
    ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls
  FROM current_rum_by_url_and_weight
  GROUP BY url
)

SELECT
  v.url,
  v.views,
  c.avglcp,
  c.avgcls,
  c.avginp,
  c.avgfid,
  COALESCE(s.actions, 0) AS submissions
FROM view_urls AS v
LEFT JOIN submission_urls AS s
  ON
    v.url = s.url
    AND (
      (
        v.source IS NULL AND (
          s.checkpoint = 'formsubmit' AND (
            s.source LIKE '%.form%'
            OR s.source LIKE '%mktoForm%'
            OR s.source LIKE '%.marketo%'
          )
        )
      ) OR (
        v.source IS NOT NULL
      )
    )
LEFT JOIN current_rum_by_url AS c
  ON
    v.url = c.url
    AND v.source IS NOT NULL
ORDER BY v.views DESC -- noqa: PRS
LIMIT CAST(@limit AS INT64)
--- url: the URL of the page that is getting traffic
--- views: the number of form views
--- avglcp: 75th percentile of the Largest Contentful Paint metric in milliseconds in the current period
--- avgcls: 75th percentile value of the Cumulative Layout Shift metric in the current period
--- avginp: 75th percentile value of the Interaction to Next Paint metric in milliseconds in the current period
--- avgfid: 75th percentile value of the First Input Delay metric in milliseconds in the current period
--- submissions: the number of form submissions
