--- description: Get Helix RUM data for a given domain or owner/repo combination
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=21500
--- limit: 10
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- timezone: UTC
--- url: -
--- owner: -
--- repo: -
--- device: all
--- rising: false
--- domainkey: secret

CREATE TEMP FUNCTION FILTERCLASS(user_agent STRING, device STRING)
RETURNS BOOLEAN
AS (
  device = "all"
  OR (
    device = "desktop"
    AND user_agent NOT LIKE "%Mobile%"
    AND user_agent LIKE "Mozilla%"
  )
  OR (device = "mobile" AND user_agent LIKE "%Mobile%")
  OR (device = "bot" AND user_agent NOT LIKE "Mozilla%")
);


WITH
current_data AS (
  SELECT * FROM
    helix_rum.EVENTS_V3(
      @url, # domain or URL
      CAST(@offset AS INT64), # not used, offset in days from today
      CAST(@interval AS INT64), # interval in days to consider
      @startdate, # not used, start date
      @enddate, # not used, end date
      @timezone, # timezone
      @device, # device class
      @domainkey
    )
),

previous_data AS (
  SELECT * FROM
    helix_rum.EVENTS_V3(
      @url, # domain or URL
      # offset in days from today
      CAST(@interval AS INT64) + CAST(@offset AS INT64),
      CAST(@interval AS INT64), # interval in days to consider
      FORMAT_DATE("%F", DATE_SUB(@startdate, INTERVAL ABS(DATE_DIFF(DATE(@enddate, @timezone), DATE(@startdate, @timezone), DAY)) DAY)), # not used, start date
      @startdate, # not used, end date
      @timezone, # timezone
      @device, # device class
      @domainkey
    )
),

current_rum_by_id AS (
  SELECT
    id,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(inp) < 200, TRUE, FALSE)) AS inpgood,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(fid) > 500, TRUE, FALSE)) AS inpbad,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) > 0.25, TRUE, FALSE)) AS clsbad,
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
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  WHERE
    url LIKE CONCAT(
      "https://", @url, "%"
    ) OR url LIKE CONCAT(
      "https://%", @repo, "--", @owner, ".hlx%/"
    ) OR (@url = "-" AND @repo = "-" AND @owner = "-")
  GROUP BY id
),

previous_rum_by_id AS (
  SELECT
    id,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(inp) < 200, TRUE, FALSE)) AS inpgood,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(inp) IS NULL, NULL, IF(MAX(inp) > 500, TRUE, FALSE)) AS inpbad,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) > 0.25, TRUE, FALSE)) AS clsbad,
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
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM previous_data
  WHERE
    url LIKE CONCAT(
      "https://", @url, "%"
    ) OR url LIKE CONCAT(
      "https://%", @repo, "--", @owner, ".hlx%/"
    ) OR (@url = "-" AND @repo = "-" AND @owner = "-")
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
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
    CAST(APPROX_QUANTILES(inp, 100)[OFFSET(75)] AS INT64) AS avginp,
    ROUND(APPROX_QUANTILES(cls, 100)[OFFSET(75)], 3) AS avgcls,
    COUNT(id) AS events
  FROM current_rum_by_id
  GROUP BY url, weight
  ORDER BY events DESC
),

previous_rum_by_url_and_weight AS (
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
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
    CAST(APPROX_QUANTILES(inp, 100)[OFFSET(75)] AS INT64) AS avginp,
    ROUND(APPROX_QUANTILES(cls, 100)[OFFSET(75)], 3) AS avgcls,
    COUNT(id) AS events
  FROM previous_rum_by_id
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
    SUM(lcpbad * weight) / SUM(weight) AS lcpbad,
    SUM(fidbad * weight) / SUM(weight) AS fidbad,
    SUM(inpbad * weight) / SUM(weight) AS inpbad,
    SUM(clsbad * weight) / SUM(weight) AS clsbad,
    SUM(avglcp * weight) / SUM(weight) AS avglcp,
    SUM(avgfid * weight) / SUM(weight) AS avgfid,
    SUM(avginp * weight) / SUM(weight) AS avginp,
    ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls,
    SUM(events * weight) AS pageviews

  FROM current_rum_by_url_and_weight
  GROUP BY url
  ORDER BY pageviews DESC
),

previous_rum_by_url AS (
  SELECT
    url,
    SUM(lcpgood * weight) / SUM(weight) AS lcpgood,
    SUM(fidgood * weight) / SUM(weight) AS fidgood,
    SUM(inpgood * weight) / SUM(weight) AS inpgood,
    SUM(clsgood * weight) / SUM(weight) AS clsgood,
    SUM(lcpbad * weight) / SUM(weight) AS lcpbad,
    SUM(fidbad * weight) / SUM(weight) AS fidbad,
    SUM(inpbad * weight) / SUM(weight) AS inpbad,
    SUM(clsbad * weight) / SUM(weight) AS clsbad,
    SUM(avglcp * weight) / SUM(weight) AS avglcp,
    SUM(avgfid * weight) / SUM(weight) AS avgfid,
    SUM(avginp * weight) / SUM(weight) AS avginp,
    ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls,
    SUM(events * weight) AS pageviews

  FROM previous_rum_by_url_and_weight
  GROUP BY url
  ORDER BY pageviews DESC
),

current_event_count AS (
  SELECT SUM(events) AS allevents FROM (
    SELECT
      id,
      MAX(weight) AS events
    FROM current_data
    GROUP BY id
  )
),

previous_event_count AS (
  SELECT SUM(events) AS allevents FROM (
    SELECT
      id,
      MAX(weight) AS events
    FROM previous_data
    GROUP BY id
  )
),

current_truncated_rum_by_url AS (
  SELECT
    CAST(SUM(ranked.lcpgood * pageviews) / SUM(pageviews) AS INT64) AS lcpgood,
    CAST(SUM(ranked.fidgood * pageviews) / SUM(pageviews) AS INT64) AS fidgood,
    CAST(SUM(ranked.inpgood * pageviews) / SUM(pageviews) AS INT64) AS inpgood,
    CAST(SUM(ranked.clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
    CAST(SUM(ranked.lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad,
    CAST(SUM(ranked.fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad,
    CAST(SUM(ranked.inpbad * pageviews) / SUM(pageviews) AS INT64) AS inpbad,
    CAST(SUM(ranked.clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
    CAST(SUM(ranked.avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp,
    CAST(SUM(ranked.avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid,
    CAST(SUM(ranked.avginp * pageviews) / SUM(pageviews) AS INT64) AS avginp,
    ROUND(SUM(ranked.avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
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
      lcpbad,
      fidbad,
      inpbad,
      clsbad,
      avglcp,
      avgfid,
      avginp,
      avgcls,
      url,
      ROW_NUMBER() OVER (ORDER BY pageviews DESC) AS rank
    FROM current_rum_by_url) AS ranked,
    current_event_count
  GROUP BY url
),

previous_truncated_rum_by_url AS (
  SELECT
    CAST(SUM(ranked.lcpgood * pageviews) / SUM(pageviews) AS INT64) AS lcpgood,
    CAST(SUM(ranked.fidgood * pageviews) / SUM(pageviews) AS INT64) AS fidgood,
    CAST(SUM(ranked.inpgood * pageviews) / SUM(pageviews) AS INT64) AS inpgood,
    CAST(SUM(ranked.clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
    CAST(SUM(ranked.lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad,
    CAST(SUM(ranked.fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad,
    CAST(SUM(ranked.inpbad * pageviews) / SUM(pageviews) AS INT64) AS inpbad,
    CAST(SUM(ranked.clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
    CAST(SUM(ranked.avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp,
    CAST(SUM(ranked.avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid,
    CAST(SUM(ranked.avginp * pageviews) / SUM(pageviews) AS INT64) AS avginp,
    ROUND(SUM(ranked.avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
    SUM(ranked.pageviews) AS pageviews,
    100 * SUM(pageviews) / MAX(previous_event_count.allevents) AS rumshare,
    IF(ranked.rank > @limit AND NOT @rising, "Other", ranked.url) AS url
  FROM
    (SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY pageviews DESC) AS rank
    FROM previous_rum_by_url) AS ranked,
    previous_event_count
  GROUP BY url
)

SELECT
  url,
  pageviews,
  pageviews_1,
  pageviews_diff,
  lcpgood,
  fidgood,
  inpgood,
  clsgood,
  lcpbad,
  fidbad,
  inpbad,
  clsbad,
  avglcp,
  avgfid,
  avginp,
  avgcls,
  rumshare,
  lcpgood_1,
  fidgood_1,
  inpgood_1,
  clsgood_1,
  lcpbad_1,
  fidbad_1,
  inpbad_1,
  clsbad_1,
  avglcp_1,
  avgfid_1,
  avginp_1,
  avgcls_1,
  rumshare_1,
  url_1
FROM (
  SELECT
    current_truncated_rum_by_url.pageviews AS pageviews,
    previous_truncated_rum_by_url.pageviews AS pageviews_1,
    current_truncated_rum_by_url.lcpgood AS lcpgood,
    current_truncated_rum_by_url.fidgood AS fidgood,
    current_truncated_rum_by_url.inpgood AS inpgood,
    current_truncated_rum_by_url.clsgood AS clsgood,
    current_truncated_rum_by_url.lcpbad AS lcpbad,
    current_truncated_rum_by_url.fidbad AS fidbad,
    current_truncated_rum_by_url.inpbad AS inpbad,
    current_truncated_rum_by_url.clsbad AS clsbad,
    current_truncated_rum_by_url.avglcp AS avglcp,
    current_truncated_rum_by_url.avgfid AS avgfid,
    current_truncated_rum_by_url.avginp AS avginp,
    current_truncated_rum_by_url.avgcls AS avgcls,
    current_truncated_rum_by_url.rumshare AS rumshare,
    previous_truncated_rum_by_url.lcpgood AS lcpgood_1,
    previous_truncated_rum_by_url.fidgood AS fidgood_1,
    previous_truncated_rum_by_url.inpgood AS inpgood_1,
    previous_truncated_rum_by_url.clsgood AS clsgood_1,
    previous_truncated_rum_by_url.lcpbad AS lcpbad_1,
    previous_truncated_rum_by_url.fidbad AS fidbad_1,
    previous_truncated_rum_by_url.inpbad AS inpbad_1,
    previous_truncated_rum_by_url.clsbad AS clsbad_1,
    previous_truncated_rum_by_url.avglcp AS avglcp_1,
    previous_truncated_rum_by_url.avgfid AS avgfid_1,
    previous_truncated_rum_by_url.avginp AS avginp_1,
    previous_truncated_rum_by_url.avgcls AS avgcls_1,
    previous_truncated_rum_by_url.rumshare AS rumshare_1,
    previous_truncated_rum_by_url.url AS url_1,
    ROW_NUMBER() OVER (
      ORDER BY
        IF(
          @rising,
          COALESCE(
            current_truncated_rum_by_url.pageviews, 0
          ) - COALESCE(previous_truncated_rum_by_url.pageviews, 0),
          0
        ) DESC,
        current_truncated_rum_by_url.pageviews DESC
    ) AS rank,
    COALESCE(
      current_truncated_rum_by_url.url, previous_truncated_rum_by_url.url
    ) AS url,
    COALESCE(
      current_truncated_rum_by_url.pageviews, 0
    ) - COALESCE(previous_truncated_rum_by_url.pageviews, 0) AS pageviews_diff
  FROM
    current_truncated_rum_by_url FULL OUTER JOIN previous_truncated_rum_by_url
    ON current_truncated_rum_by_url.url = previous_truncated_rum_by_url.url
  ORDER BY
    IF(current_truncated_rum_by_url.url = "Other", 1, 0),
    IF(
      @rising,
      COALESCE(
        current_truncated_rum_by_url.pageviews, 0
      ) - COALESCE(previous_truncated_rum_by_url.pageviews, 0),
      0
    ) DESC,
    current_truncated_rum_by_url.pageviews DESC,
    previous_truncated_rum_by_url.pageviews DESC
) WHERE
  rank <= @limit OR url = "Other" OR @rising
