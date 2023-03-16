--- description: Get Helix RUM data for a given domain or owner/repo combination
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 10
--- interval: 30
--- domain: -
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
  ) OR
  (device = "mobile" AND user_agent LIKE "%Mobile%") OR
  (device = "bot" AND user_agent NOT LIKE "Mozilla%"));


WITH
current_data AS (
  SELECT * FROM helix_rum.EVENTS_V3(
    @domain, # domain or URL
    0, # not used, offset in days from today
    CAST(@interval AS INT64), # interval in days to consider
    "2022-02-01", # not used, start date
    "2022-05-28", # not used, end date
    "GMT", # timezone
    @device, # device class
    @domainkey
  )
),

previous_data AS (
  SELECT * FROM helix_rum.EVENTS_V3(
    @domain, # domain or URL
    # offset in days from today (used only if generation filter is not used)
    IF(@generationb = "-", CAST(@interval AS INT64), 0),
    CAST(@interval AS INT64), # interval in days to consider
    "2022-02-01", # not used, start date
    "2022-05-28", # not used, end date
    "GMT", # timezone
    @device, # device class
    @domainkey
  )
),

current_rum_by_id AS (
  SELECT
    id,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) > 0.25, TRUE, FALSE)) AS clsbad,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    IF(
      @domain = "-" AND @repo = "-" AND @owner = "-",
      REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1),
      MAX(url)
    ) AS url,
    MAX(lcp) AS lcp,
    MAX(fid) AS fid,
    MAX(cls) AS cls,
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM current_data
  WHERE
    url LIKE CONCAT(
      "https://", @domain, "%"
    ) OR url LIKE CONCAT(
      "https://%", @repo, "--", @owner, ".hlx%/"
    ) OR (@domain = "-" AND @repo = "-" AND @owner = "-")
  GROUP BY id
),

previous_rum_by_id AS (
  SELECT
    id,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(lcp) IS NULL, NULL, IF(MAX(lcp) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(fid) IS NULL, NULL, IF(MAX(fid) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(cls) IS NULL, NULL, IF(MAX(cls) > 0.25, TRUE, FALSE)) AS clsbad,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    IF(
      @domain = "-" AND @repo = "-" AND @owner = "-",
      REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1),
      MAX(url)
    ) AS url,
    MAX(lcp) AS lcp,
    MAX(fid) AS fid,
    MAX(cls) AS cls,
    MAX(referer) AS referer,
    MAX(weight) AS weight
  FROM previous_data
  WHERE
    url LIKE CONCAT(
      "https://", @domain, "%"
    ) OR url LIKE CONCAT(
      "https://%", @repo, "--", @owner, ".hlx%/"
    ) OR (@domain = "-" AND @repo = "-" AND @owner = "-")
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
        COUNTIF(clsbad IS NOT NULL) != 0,
        COUNTIF(clsbad = TRUE) / COUNTIF(clsbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS clsbad,
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
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
        COUNTIF(clsbad IS NOT NULL) != 0,
        COUNTIF(clsbad = TRUE) / COUNTIF(clsbad IS NOT NULL),
        NULL
      ) AS INT64
    ) AS clsbad,
    CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64) AS avglcp,
    CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
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
    SUM(clsgood * weight) / SUM(weight) AS clsgood,
    SUM(lcpbad * weight) / SUM(weight) AS lcpbad,
    SUM(fidbad * weight) / SUM(weight) AS fidbad,
    SUM(clsbad * weight) / SUM(weight) AS clsbad,
    SUM(avglcp * weight) / SUM(weight) AS avglcp,
    SUM(avgfid * weight) / SUM(weight) AS avgfid,
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
    SUM(clsgood * weight) / SUM(weight) AS clsgood,
    SUM(lcpbad * weight) / SUM(weight) AS lcpbad,
    SUM(fidbad * weight) / SUM(weight) AS fidbad,
    SUM(clsbad * weight) / SUM(weight) AS clsbad,
    SUM(avglcp * weight) / SUM(weight) AS avglcp,
    SUM(avgfid * weight) / SUM(weight) AS avgfid,
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
    CAST(SUM(ranked.clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
    CAST(SUM(ranked.lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad,
    CAST(SUM(ranked.fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad,
    CAST(SUM(ranked.clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
    CAST(SUM(ranked.avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp,
    CAST(SUM(ranked.avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid,
    ROUND(SUM(ranked.avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
    SUM(ranked.pageviews) AS pageviews,
    100 * SUM(pageviews) / MAX(current_event_count.allevents) AS rumshare,
    IF(ranked.rank > @limit AND NOT @rising, "Other", ranked.url) AS url
  FROM
    (SELECT
      pageviews,
      lcpgood,
      fidgood,
      clsgood,
      lcpbad,
      fidbad,
      clsbad,
      avglcp,
      avgfid,
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
    CAST(SUM(ranked.clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
    CAST(SUM(ranked.lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad,
    CAST(SUM(ranked.fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad,
    CAST(SUM(ranked.clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
    CAST(SUM(ranked.avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp,
    CAST(SUM(ranked.avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid,
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
  clsgood,
  lcpbad,
  fidbad,
  clsbad,
  avglcp,
  avgfid,
  avgcls,
  rumshare,
  lcpgood_1,
  fidgood_1,
  clsgood_1,
  lcpbad_1,
  fidbad_1,
  clsbad_1,
  avglcp_1,
  avgfid_1,
  avgcls_1,
  rumshare_1,
  url_1
FROM (
  SELECT
    current_truncated_rum_by_url.pageviews AS pageviews,
    previous_truncated_rum_by_url.pageviews AS pageviews_1,
    current_truncated_rum_by_url.lcpgood AS lcpgood,
    current_truncated_rum_by_url.fidgood AS fidgood,
    current_truncated_rum_by_url.clsgood AS clsgood,
    current_truncated_rum_by_url.lcpbad AS lcpbad,
    current_truncated_rum_by_url.fidbad AS fidbad,
    current_truncated_rum_by_url.clsbad AS clsbad,
    current_truncated_rum_by_url.avglcp AS avglcp,
    current_truncated_rum_by_url.avgfid AS avgfid,
    current_truncated_rum_by_url.avgcls AS avgcls,
    current_truncated_rum_by_url.rumshare AS rumshare,
    previous_truncated_rum_by_url.lcpgood AS lcpgood_1,
    previous_truncated_rum_by_url.fidgood AS fidgood_1,
    previous_truncated_rum_by_url.clsgood AS clsgood_1,
    previous_truncated_rum_by_url.lcpbad AS lcpbad_1,
    previous_truncated_rum_by_url.fidbad AS fidbad_1,
    previous_truncated_rum_by_url.clsbad AS clsbad_1,
    previous_truncated_rum_by_url.avglcp AS avglcp_1,
    previous_truncated_rum_by_url.avgfid AS avgfid_1,
    previous_truncated_rum_by_url.avgcls AS avgcls_1,
    previous_truncated_rum_by_url.rumshare AS rumshare_1,
    previous_truncated_rum_by_url.url AS url_1,
    ROW_NUMBER() OVER (ORDER BY
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
