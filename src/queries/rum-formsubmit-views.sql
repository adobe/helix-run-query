
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
current_data AS (
  SELECT * FROM
    helix_rum.EVENTS_V5(
      @url, # domain or URL
      CAST(@offset AS INT64), # not used, offset in days from today
      CAST(@interval AS INT64), # interval in days to consider
      @startdate, # not used, start date
      @enddate, # not used, end date
      @timezone, # timezone
      'all',
      @domainkey
    )
),
current_rum_by_id AS (
  SELECT
    id,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) <= 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) <= 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(inp) <= 200, TRUE, FALSE)) AS inpgood,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) <= 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(ttfb) IS NULL, NULL, IF(MAX(ttfb) <= 800, TRUE, FALSE)) AS ttfbgood,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) >= 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) >= 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(fid) >= 500, TRUE, FALSE)) AS inpbad,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) >= 0.25, TRUE, FALSE)) AS clsbad,
    IF(MAX(ttfb) IS NULL, NULL, IF(MAX(ttfb) >= 1800, TRUE, FALSE)) AS ttfbbad,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    IF(
      @url = "-" AND @repo = "-" AND @owner = "-",
      REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1),
      MAX(url)
    ) AS url,
    MAX(lcp) AS lcp,
    MAX(fid) AS fid,
    MAX(inp) AS inp,
    MAX(cls) AS cls,
    MAX(ttfb) AS ttfb,
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  GROUP BY id
),
current_rum_by_url_and_weight AS (
  SELECT
    weight,
    url,
    CAST(
      100 * IF(
        COUNTIF(lcpgood IS NOT NULL) != 0,
        COUNTIF(lcpgood = TRUE) / COUNTIF(lcpgood IS NOT NULL),
        NULL
      ) AS INT64
    ) AS lcpgood,
    CAST(
      100 * IF(
        COUNTIF(fidgood IS NOT NULL) != 0,
        COUNTIF(fidgood = TRUE) / COUNTIF(fidgood IS NOT NULL),
        NULL
      ) AS INT64
    ) AS fidgood,
    CAST(
      100 * IF(
        COUNTIF(inpgood IS NOT NULL) != 0,
        COUNTIF(inpgood = TRUE) / COUNTIF(inpgood IS NOT NULL),
        NULL
      ) AS INT64
    ) AS inpgood,
    CAST(
      100 * IF(
        COUNTIF(clsgood IS NOT NULL) != 0,
        COUNTIF(clsgood = TRUE) / COUNTIF(clsgood IS NOT NULL),
        NULL
      ) AS INT64
    ) AS clsgood,
    CAST(
      100 * IF(
        COUNTIF(ttfbgood IS NOT NULL) != 0,
        COUNTIF(ttfbgood = TRUE) / COUNTIF(ttfbgood IS NOT NULL),
        NULL
      ) AS INT64
    ) AS ttfbgood,
    CAST(
      100 * IF(
        COUNTIF(lcpbad IS NOT NULL) != 0,
        COUNTIF(lcpbad = TRUE) / COUNTIF(lcpbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS lcpbad,
    CAST(
      100 * IF(
        COUNTIF(fidbad IS NOT NULL) != 0,
        COUNTIF(fidbad = TRUE) / COUNTIF(fidbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS fidbad,
    CAST(
      100 * IF(
        COUNTIF(inpbad IS NOT NULL) != 0,
        COUNTIF(inpbad = TRUE) / COUNTIF(inpbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS inpbad,
    CAST(
      100 * IF(
        COUNTIF(clsbad IS NOT NULL) != 0,
        COUNTIF(clsbad = TRUE) / COUNTIF(clsbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS clsbad,
    CAST(
      100 * IF(
        COUNTIF(ttfbbad IS NOT NULL) != 0,
        COUNTIF(ttfbbad = TRUE) / COUNTIF(ttfbbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS ttfbbad,
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
    CAST(APPROX_QUANTILES(inp, 100)[OFFSET(75)] AS INT64) AS avginp,
    ROUND(APPROX_QUANTILES(cls, 100)[OFFSET(75)], 3) AS avgcls,
    CAST(APPROX_QUANTILES(ttfb, 100)[OFFSET(75)] AS INT64) AS avgttfb,
    COUNT(id) AS events
  FROM current_rum_by_id
  GROUP BY url, weight
  ORDER BY events DESC
),
current_rum_by_url AS (
  SELECT
    url,
    SUM(lcpgood * weight) / SUM(weight) AS lcpgood,
    SUM(fidgood * weight) / SUM(weight) AS fidgood,
    SUM(inpgood * weight) / SUM(weight) AS inpgood,
    SUM(clsgood * weight) / SUM(weight) AS clsgood,
    SUM(ttfbgood * weight) / SUM(weight) AS ttfbgood,
    SUM(lcpbad * weight) / SUM(weight) AS lcpbad,
    SUM(fidbad * weight) / SUM(weight) AS fidbad,
    SUM(inpbad * weight) / SUM(weight) AS inpbad,
    SUM(clsbad * weight) / SUM(weight) AS clsbad,
    SUM(ttfbbad * weight) / SUM(weight) AS ttfbbad,
    SUM(avglcp * weight) / SUM(weight) AS avglcp,
    SUM(avgfid * weight) / SUM(weight) AS avgfid,
    SUM(avginp * weight) / SUM(weight) AS avginp,
    ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls,
    SUM(avgttfb * weight) / SUM(weight) AS avgttfb,
    SUM(events * weight) AS pageviews

  FROM current_rum_by_url_and_weight
  GROUP BY url
  ORDER BY pageviews DESC
),
current_truncated_rum_by_url AS (
  SELECT
    CAST(SUM(ranked.lcpgood * pageviews) / SUM(pageviews) AS INT64) AS lcpgood,
    CAST(SUM(ranked.fidgood * pageviews) / SUM(pageviews) AS INT64) AS fidgood,
    CAST(SUM(ranked.inpgood * pageviews) / SUM(pageviews) AS INT64) AS inpgood,
    CAST(SUM(ranked.clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
    CAST(SUM(ranked.ttfbgood * pageviews) / SUM(pageviews) AS INT64) AS ttfbgood,
    CAST(SUM(ranked.lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad,
    CAST(SUM(ranked.fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad,
    CAST(SUM(ranked.inpbad * pageviews) / SUM(pageviews) AS INT64) AS inpbad,
    CAST(SUM(ranked.clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
    CAST(SUM(ranked.ttfbbad * pageviews) / SUM(pageviews) AS INT64) AS ttfbbad,
    CAST(SUM(ranked.avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp,
    CAST(SUM(ranked.avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid,
    CAST(SUM(ranked.avginp * pageviews) / SUM(pageviews) AS INT64) AS avginp,
    ROUND(SUM(ranked.avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
    CAST(SUM(ranked.avgttfb * pageviews) / SUM(pageviews) AS INT64) AS avgttfb,
    SUM(ranked.pageviews) AS pageviews,
    100 * SUM(pageviews) / MAX(current_event_count.allevents) AS rumshare,
    IF(ranked.rank > @limit AND NOT @rising, "Other", ranked.url) AS url
  FROM
    (SELECT
      pageviews,
      lcpgood,
      fidgood,
      inpgood,
      clsgood,
      ttfbgood,
      lcpbad,
      fidbad,
      inpbad,
      clsbad,
      ttfbbad,
      avglcp,
      avgfid,
      avginp,
      avgcls,
      avgttfb,
      url,
      ROW_NUMBER() OVER (ORDER BY pageviews DESC) AS rank
    FROM current_rum_by_url) AS ranked,
    current_event_count
  GROUP BY url
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
  GROUP BY url, checkpoint, source
),


SELECT
  v.url,
  v.pageviews
  COALESCE(s.actions, 0) AS submissions
FROM submission_urls AS s
LEFT JOIN current_truncated_rum_by_url AS v ON s.url = v.url
ORDER BY v.pageviews DESC -- noqa: PRS
LIMIT CAST(@limit AS INT64)
--- url: the URL of the page that is getting traffic
--- views: the number of form views
--- avglcp: 75th percentile of the Largest Contentful Paint metric in milliseconds in the current period
--- avgcls: 75th percentile value of the Cumulative Layout Shift metric in the current period
--- avginp: 75th percentile value of the Interaction to Next Paint metric in milliseconds in the current period
--- avgfid: 75th percentile value of the First Input Delay metric in milliseconds in the current period
--- submissions: the number of form submissions
