--- description: Show content reach and persistence over time
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 60
--- offset: 0
--- domain: -
DECLARE upperdate STRING DEFAULT CONCAT(
  CAST(
    EXTRACT(
      YEAR FROM TIMESTAMP_SUB(
        CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY
      )
    ) AS String
  ),
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL CAST(@offset AS INT64) DAY)) AS String), 2, "0"));

DECLARE lowerdate STRING DEFAULT CONCAT(
  CAST(
    EXTRACT(
      YEAR FROM TIMESTAMP_SUB(
        CURRENT_TIMESTAMP(),
        INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY
      )
    ) AS String
  ),
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY
    )) AS String), 2, "0"));

DECLARE uppertimestamp STRING DEFAULT CAST(
  UNIX_MICROS(
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)
  ) AS STRING
);

DECLARE lowertimestamp STRING DEFAULT CAST(
  UNIX_MICROS(
    TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY
    )
  ) AS STRING
);

CREATE TEMP FUNCTION LABELSCORE(score FLOAT64)
RETURNS STRING
AS (
  IF(
    score <= 1,
    "A",
    IF(score <= 2, "B", IF(score <= 3, "C", IF(score <= 4, "D", "F")))
  )
);

WITH visits AS (
  SELECT
    id,
    REGEXP_REPLACE(ANY_VALUE(url), "\\?.*$", "") AS url,
    ANY_VALUE(hostname) AS host,
    TIMESTAMP_TRUNC(MAX(time), DAY) AS time,
    MAX(weight) AS weight,
    MAX(lcp) AS lcp,
    MAX(cls) AS cls,
    MAX(fid) AS fid,
    MAX(IF(checkpoint = "top", 1, 0)) AS top,
    MAX(IF(checkpoint = "load", 1, 0)) AS load,
    MAX(IF(checkpoint = "click", 1, 0)) AS click
  FROM helix_rum.CLUSTER_EVENTS(
    @domain,
    CAST(@offset AS INT64),
    CAST(@interval AS INT64),
    "",
    "",
    "GMT",
    "all",
    "-"
  )
  GROUP BY id
),

urldays AS (
  SELECT
    time,
    url,
    MAX(host) AS host,
    COUNT(id) AS events,
    SUM(weight) AS visits,
    AVG(lcp) AS lcp,
    AVG(cls) AS cls,
    AVG(fid) AS fid,
    LEAST(IF(SUM(top) > 0, SUM(load) / SUM(top), 0), 1) AS load,
    LEAST(IF(SUM(top) > 0, SUM(click) / SUM(top), 0), 1) AS click
  FROM visits # FULL JOIN days ON (days.time = visits.time)
  GROUP BY time, url
),

steps AS (
  SELECT
    time,
    url,
    host,
    events,
    visits,
    lcp,
    cls,
    fid,
    load,
    click,
    TIMESTAMP_DIFF(
      time, LAG(time) OVER(PARTITION BY url ORDER BY time), DAY
    ) AS step
  FROM urldays
),

chains AS (
  SELECT
    time,
    url,
    host,
    events,
    visits,
    lcp,
    cls,
    fid,
    load,
    click,
    step,
    COUNTIF(step = 1) OVER(PARTITION BY url ORDER BY time) AS chain
  FROM steps
),

urlchains AS (
  SELECT
    url,
    time,
    chain,
    events,
    visits
  FROM chains
  ORDER BY chain DESC
),

powercurve AS (
  SELECT
    MAX(chain) AS persistence,
    COUNT(url) AS reach
  FROM urlchains
  GROUP BY chain
  ORDER BY MAX(chain) ASC
  LIMIT 31 OFFSET 1
),

powercurvequintiles AS (
  SELECT
    APPROX_QUANTILES(DISTINCT reach, 3) AS reach,
    APPROX_QUANTILES(DISTINCT persistence, 3) AS persistence
  FROM (
    SELECT * FROM (
      SELECT
        host,
        COUNTIF(chain = 1) AS reach,
        COUNTIF(chain = 7) AS persistence
      FROM chains
      GROUP BY host
    )
    WHERE reach > 0 AND persistence > 0 AND host IS NOT NULL
  )
),

cwvquintiles AS (
  SELECT
    APPROX_QUANTILES(DISTINCT lcp, 3) AS lcp,
    APPROX_QUANTILES(DISTINCT cls, 3) AS cls,
    APPROX_QUANTILES(DISTINCT fid, 3) AS fid,
    APPROX_QUANTILES(DISTINCT load, 3) AS load,
    APPROX_QUANTILES(DISTINCT click, 3) AS click
  FROM chains
),

cwvquintiletable AS (
  SELECT
    num,
    lcp[OFFSET(num)] AS lcp,
    cls[OFFSET(num)] AS cls,
    fid[OFFSET(num)] AS fid,
    load[OFFSET(3 - num)] AS load,
    click[OFFSET(3 - num)] AS click
  FROM cwvquintiles INNER JOIN UNNEST(GENERATE_ARRAY(0, 3)) AS num
),

powercurvequintiletable AS (
  SELECT
    num,
    reach[OFFSET(3 - num)] AS reach,
    persistence[OFFSET(3 - num)] AS persistence
  FROM powercurvequintiles INNER JOIN UNNEST(GENERATE_ARRAY(0, 3)) AS num
),

quintiletable AS (
  # SELECT * FROM powercurve
  SELECT
    cls,
    lcp,
    fid,
    load,
    click,
    reach,
    persistence,
    cwvquintiletable.num AS num
  FROM
    powercurvequintiletable
  INNER JOIN
    cwvquintiletable ON powercurvequintiletable.num = cwvquintiletable.num
),

lookmeup AS (
  SELECT
    host,
    LABELSCORE(
      (
        (
          (clsscore + lcpscore + fidscore) / 3
        ) + ((reachscore + persistencescore + loadscore) / 3) + (clickscore)
      ) / 3
    ) AS experiencescore,
    LABELSCORE((clsscore + lcpscore + fidscore) / 3) AS perfscore,
    LABELSCORE(
      (reachscore + persistencescore + loadscore) / 3
    ) AS audiencescore,
    LABELSCORE(clickscore) AS engagementscore
  FROM (
    SELECT
      host,
      chained.cls AS cls,
      # (SELECT num FROM quintiletable WHERE quintiletable.CLS <= chained.CLS) AS clsscore,
      chained.lcp AS lcp,
      chained.fid AS fid,
      chained.load AS load,
      chained.click AS click,
      chained.reach AS reach,
      chained.persistence AS persistence,
      (
        SELECT MAX(num)
        FROM
          (SELECT num FROM quintiletable WHERE quintiletable.cls <= chained.cls)
      ) AS clsscore,
      (
        SELECT MAX(num)
        FROM
          (SELECT num FROM quintiletable WHERE quintiletable.lcp <= chained.lcp)
      ) AS lcpscore,
      (
        SELECT MAX(num)
        FROM
          (SELECT num FROM quintiletable WHERE quintiletable.fid <= chained.fid)
      ) AS fidscore,
      (
        SELECT MIN(num)
        FROM
          (
            SELECT num
            FROM quintiletable WHERE chained.load >= quintiletable.load
          )
      ) AS loadscore,
      (
        SELECT MIN(num)
        FROM
          (
            SELECT num
            FROM quintiletable WHERE chained.click >= quintiletable.click
          )
      ) AS clickscore,
      (
        SELECT MIN(num)
        FROM
          (
            SELECT num
            FROM quintiletable WHERE chained.reach >= quintiletable.reach
          )
      ) AS reachscore,
      (
        SELECT MIN(num)
        FROM
          (
            SELECT num
            FROM
              quintiletable
            WHERE chained.persistence >= quintiletable.persistence
          )
      ) AS persistencescore
    FROM (
        SELECT
          host,
          AVG(cls) AS cls,
          AVG(lcp) AS lcp,
          AVG(fid) AS fid,
          AVG(load) AS load,
          AVG(click) AS click,
          COUNTIF(chain = 1) AS reach,
          COUNTIF(chain = 7) AS persistence
        FROM chains
        WHERE (host = @domain OR @domain = "-")
          AND host IS NOT NULL
          AND host NOT LIKE "%.hlx.%"
          AND host != "localhost"
        GROUP BY host
        ORDER BY host DESC
      ) AS chained
  )
  WHERE host = @domain OR (@domain = "-"
    AND clsscore IS NOT NULL
    AND fidscore IS NOT NULL
    AND lcpscore IS NOT NULL
    AND clickscore IS NOT NULL
    AND loadscore IS NOT NULL
    AND reachscore IS NOT NULL
    AND persistencescore IS NOT NULL
  )
)

#SELECT MIN(num) FROM (SELECT * FROM quintiletable WHERE 0.9841262752758466 >= quintiletable.click)
SELECT * FROM lookmeup
