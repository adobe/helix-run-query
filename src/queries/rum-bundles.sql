--- description: get raw rum data for a given day and base URL
--- Cache-Control: max-age=86400
--- url: -
--- startdate: 2024-01-01
--- enddate: 2024-01-02
--- after: unset
--- limit: 1000
--- domainkey: secret

WITH alldata AS (
  SELECT
    id,
    url,
    FORMAT_TIMESTAMP('%Y-%m-%dT%X%Ez', MIN(time)) AS time, -- noqa: RF04
    CAST(ANY_VALUE(weight) AS INT64) AS weight,
    ANY_VALUE(user_agent) AS user_agent,
    ARRAY_AGG(
      STRUCT(
        FORMAT_TIMESTAMP('%Y-%m-%dT%X%Ez', time) AS time, -- noqa: RF04
        CASE
          WHEN LCP IS NOT NULL THEN 'cwv-lcp'
          WHEN INP IS NOT NULL THEN 'cwv-inp'
          WHEN CLS IS NOT NULL THEN 'cwv-cls'
          WHEN TTFB IS NOT NULL THEN 'cwv-ttfb'
          ELSE checkpoint
        END AS checkpoint,
        source,
        target,
        CASE
          WHEN LCP IS NOT NULL THEN LCP
          WHEN INP IS NOT NULL THEN INP
          WHEN CLS IS NOT NULL THEN CLS
          WHEN TTFB IS NOT NULL THEN TTFB
        END AS value -- noqa: RF04
      )
    ) AS events,
    # row number
    ROW_NUMBER() OVER (ORDER BY MIN(time) ASC, id ASC) AS rownum,
    id = CAST(@after AS STRING) AS is_cursor
  FROM
    helix_rum.EVENTS_V5(
      @url,
      -1,
      -1,
      @startdate,
      # startdate plus one day
      @startdate,
      'UTC',
      'all',
      @domainkey
    )
  GROUP BY
    id,
    url,
    TIMESTAMP_TRUNC(time, DAY)
  ORDER BY
    time ASC,
    id ASC
),

cursor_rows AS (
  SELECT MIN(rownum) AS rownum FROM alldata WHERE is_cursor
  UNION ALL
  SELECT 0 AS rownum FROM alldata WHERE @after = 'unset'
),

cursor_rownum AS (
  SELECT MIN(rownum) AS rownum FROM cursor_rows
)


# select everything from the alldata CTE, but limit to @limit rows
# and skip until the @after cursor
SELECT
  id,
  url,
  time,
  weight,
  user_agent,
  events,
  rownum
FROM
  alldata
WHERE
  (rownum > (SELECT rownum FROM cursor_rownum))
  AND (rownum <= ((SELECT rownum FROM cursor_rownum) + @limit))
ORDER BY
  rownum ASC
