--- description: Get daily page views for a site according to Helix RUM data
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- url: 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
DECLARE results NUMERIC;
CREATE OR REPLACE PROCEDURE helix_rum.UPDATE_PAGEVIEWS (
  ingranularity INT64,
  inlimit INT64,
  inoffset INT64,
  inurl STRING,
  intimezone STRING,
  indomainkey STRING,
  OUT results NUMERIC
)
BEGIN
  CREATE TEMP TABLE temp_pageviews (
    year INT64,
    month INT64,
    day INT64,
    time STRING, --noqa: RF04
    url INT64,
    pageviews NUMERIC
  )
  AS
  WITH current_data AS (
    SELECT
      *,
      CASE ingranularity
        WHEN 7 THEN TIMESTAMP_TRUNC(time, ISOWEEK)
        WHEN 30 THEN TIMESTAMP_TRUNC(time, MONTH)
        WHEN 90 THEN TIMESTAMP_TRUNC(time, QUARTER)
        WHEN 365 THEN TIMESTAMP_TRUNC(time, YEAR)
        ELSE TIMESTAMP_TRUNC(time, DAY)
      END AS date
    FROM helix_rum.PAGEVIEWS_V3(
      inurl, # url
      GREATEST((inoffset * ingranularity) - 1, 0), # offset
      inlimit * ingranularity, # days to fetch
      '2022-05-01', # not used, start date
      '2022-05-28', # not used, end date
      intimezone, # timezone
      'all', # deviceclass
      indomainkey # domain key to prevent data sharing
    )
  ),

  pageviews_by_id AS (
    SELECT
      id,
      MAX(date) AS date,
      MAX(url) AS url,
      MAX(pageviews) AS weight
    FROM current_data
    GROUP BY id
  ),

  dailydata AS (
    SELECT
      EXTRACT(YEAR FROM date) AS year,
      EXTRACT(MONTH FROM date) AS month,
      EXTRACT(DAY FROM date) AS day,
      STRING(date) AS time, -- noqa: RF04
      COUNT(url) AS urls,
      SUM(weight) AS pageviews
    FROM pageviews_by_id
    GROUP BY date
    ORDER BY date DESC
  ),

  basicdates AS (
    SELECT alldates
    FROM
      UNNEST(
        GENERATE_TIMESTAMP_ARRAY(
          (SELECT MIN(date) FROM pageviews_by_id),
          (SELECT MAX(date) FROM pageviews_by_id),
          INTERVAL 1 DAY
        )
      ) AS alldates
  ),

  dates AS (
    SELECT CASE ingranularity
      WHEN 7 THEN TIMESTAMP_TRUNC(alldates, ISOWEEK)
      WHEN 30 THEN TIMESTAMP_TRUNC(alldates, MONTH)
      WHEN 90 THEN TIMESTAMP_TRUNC(alldates, QUARTER)
      WHEN 365 THEN TIMESTAMP_TRUNC(alldates, YEAR)
      ELSE TIMESTAMP_TRUNC(alldates, DAY)
    END AS alldates FROM basicdates
    GROUP BY alldates
  ),

  finaldata AS (
    SELECT
      EXTRACT(YEAR FROM dates.alldates) AS year,
      EXTRACT(MONTH FROM dates.alldates) AS month,
      EXTRACT(DAY FROM dates.alldates) AS day,
      STRING(dates.alldates) AS time, -- noqa: RF04
      COALESCE(dailydata.urls, 0) AS distinct_urls,
      COALESCE(dailydata.pageviews, 0) AS pageviews
    FROM dates
    FULL JOIN dailydata
      ON STRING(dates.alldates) = dailydata.time
    ORDER BY dates.alldates DESC
  )

  SELECT * FROM finaldata ORDER BY time DESC;
  SET results = (SELECT SUM(pageviews) FROM (SELECT * FROM temp_pageviews));
END;
IF (CAST(@granularity AS STRING) = "auto") THEN -- noqa: PRS
    CALL helix_rum.UPDATE_PAGEVIEWS(1, CAST(@interval AS INT64), CAST(@offset AS INT64), @url, @timezone, @domainkey, results);
    IF (results > (CAST(@interval AS INT64) * 200)) THEN -- noqa: PRS
        # we have enough results, use the daily granularity
        SELECT * FROM temp_pageviews;
    ELSE -- noqa: PRS
        # we don't have enough results, zoom out
        DROP TABLE temp_pageviews;
        CALL helix_rum.UPDATE_PAGEVIEWS(7, CAST(@interval AS INT64), CAST(@offset AS INT64), @url, @timezone, @domainkey, results);
        IF (results > (CAST(@interval AS INT64) * 200)) THEN
            # we have enough results, use the weekly granularity
            SELECT * FROM temp_pageviews;
        ELSE
            # we don't have enough results, zoom out to monthly and stop
            DROP TABLE temp_pageviews;
            CALL helix_rum.UPDATE_PAGEVIEWS(30, CAST(@interval AS INT64), CAST(@offset AS INT64), @url, @timezone, @domainkey, results);
            SELECT * FROM temp_pageviews;
        END IF;
    END IF; -- noqa: PRS
ELSE -- noqa: PRS
    CALL helix_rum.UPDATE_PAGEVIEWS(CAST(@granularity AS INT64), CAST(@interval AS INT64), CAST(@offset AS INT64), @url, @timezone, @domainkey, results);
    SELECT * FROM temp_pageviews;
END IF; -- noqa: PRS
