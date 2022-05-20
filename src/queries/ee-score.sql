--- description: Show content reach and persistence over time
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 60
--- offset: 0
--- domain: -
DECLARE upperdate STRING DEFAULT CONCAT(
  CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)) AS String), 
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), 
    INTERVAL CAST(@offset AS INT64) DAY)) AS String), 2, "0"));

DECLARE lowerdate STRING DEFAULT CONCAT(
  CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS String), 
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), 
    INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS String), 2, "0"));

DECLARE uppertimestamp STRING DEFAULT CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)) AS STRING);

DECLARE lowertimestamp STRING DEFAULT CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS STRING);

CREATE TEMP FUNCTION LABELSCORE(score FLOAT64)
  RETURNS STRING
  AS (
    IF(score <= 1, "A", IF(score <= 2, "B", IF(score <= 3, "C", IF(score <= 4, "D", "F"))))
  );

WITH visits AS (
  SELECT 
    REGEXP_REPLACE(ANY_VALUE(url), "\\?.*$", "") AS url,
    ANY_VALUE(hostname) AS host,
    TIMESTAMP_TRUNC(MAX(time), DAY) AS time, 
    id, 
    MAX(weight) AS weight,
    MAX(LCP) AS LCP,
    MAX(CLS) AS CLS,
    MAX(FID) AS FID,
    MAX(IF(checkpoint = "top", 1, 0)) AS top,
    MAX(IF(checkpoint = "load", 1, 0)) AS load,
    MAX(IF(checkpoint = "click", 1, 0)) AS click
  FROM helix_rum.CLUSTER_EVENTS(
    @domain,
    CAST(@offset AS INT64),
    CAST(@interval AS INT64),
    '',
    '',
    'GMT',
    'all',
    '-'
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
    AVG(LCP) AS LCP,
    AVG(CLS) AS CLS,
    AVG(FID) AS FID,
    LEAST(IF(SUM(top) > 0, SUM(load) / SUM(top), 0), 1) AS load,
    LEAST(IF(SUM(top) > 0, SUM(click) / SUM(top), 0), 1) AS click
  FROM visits # FULL JOIN days ON (days.time = visits.time)
  GROUP BY time, url
),
steps AS (
  SELECT time, url, host, events, visits,
  LCP, CLS, FID, load, click, 
  TIMESTAMP_DIFF(time, LAG(time) OVER(PARTITION BY url ORDER BY time), DAY) AS step 
  FROM urldays
),
chains AS (
  SELECT time, url, host, events, visits, 
  LCP, CLS, FID, load, click, 
  step, 
      COUNTIF(step = 1) OVER(PARTITION BY url ORDER BY time) AS chain
      FROM steps
),
urlchains AS (
  SELECT url, time, chain, events, visits FROM chains
  ORDER BY chain DESC
),
powercurve AS (
  SELECT 
    MAX(chain) AS persistence, 
    count(url) AS reach
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
    APPROX_QUANTILES(DISTINCT LCP, 3) AS LCP,
    APPROX_QUANTILES(DISTINCT CLS, 3) AS CLS,
    APPROX_QUANTILES(DISTINCT FID, 3) AS FID,
    APPROX_QUANTILES(DISTINCT load, 3) AS load,
    APPROX_QUANTILES(DISTINCT click, 3) AS click,
  FROM chains
),
cwvquintiletable AS (
  SELECT 
    num, 
    LCP[OFFSET(num)] AS LCP,
    CLS[OFFSET(num)] AS CLS,
    FID[OFFSET(num)] AS FID,
    load[OFFSET(3 - num)] AS load,
    click[OFFSET(3 - num)] AS click,
  FROM cwvquintiles JOIN UNNEST(GENERATE_ARRAY(0, 3)) AS num
),
powercurvequintiletable AS (
  SELECT 
    num, 
    reach[OFFSET(3 - num)] AS reach,
    persistence[OFFSET(3 - num)] AS persistence,
  FROM powercurvequintiles JOIN UNNEST(GENERATE_ARRAY(0, 3)) AS num
),
quintiletable AS (
  # SELECT * FROM powercurve
  SELECT * FROM powercurvequintiletable JOIN cwvquintiletable USING (num)
),
lookmeup AS (
SELECT
  host,
  LABELSCORE((((clsscore + lcpscore + fidscore) / 3) + ((reachscore + persistencescore + loadscore) / 3) + (clickscore)) / 3) AS experiencescore,
  LABELSCORE((clsscore + lcpscore + fidscore) / 3) AS perfscore,
  LABELSCORE((reachscore + persistencescore + loadscore) / 3) AS audiencescore,
  LABELSCORE(clickscore) AS engagementscore,
  FROM (
    SELECT 
      host,
      chained.CLS AS CLS,
      # (SELECT num FROM quintiletable WHERE quintiletable.CLS <= chained.CLS) AS clsscore,
      (SELECT MAX(num) FROM (SELECT num FROM quintiletable WHERE quintiletable.CLS <= chained.CLS)) AS clsscore,
      chained.LCP AS LCP,
      (SELECT MAX(num) FROM (SELECT num FROM quintiletable WHERE quintiletable.LCP <= chained.LCP)) AS lcpscore,
      chained.FID AS FID,
      (SELECT MAX(num) FROM (SELECT num FROM quintiletable WHERE quintiletable.FID <= chained.FID)) AS fidscore,
      chained.load AS load,
      (SELECT MIN(num) FROM (SELECT num FROM quintiletable WHERE chained.load >= quintiletable.load)) AS loadscore,
      chained.click AS click,
      (SELECT MIN(num) FROM (SELECT num FROM quintiletable WHERE chained.click >= quintiletable.click)) AS clickscore,
      chained.reach AS reach,
      (SELECT MIN(num) FROM (SELECT num FROM quintiletable WHERE chained.reach >= quintiletable.reach)) AS reachscore,
      chained.persistence AS persistence,
      (SELECT MIN(num) FROM (SELECT num FROM quintiletable WHERE chained.persistence >= quintiletable.persistence)) AS persistencescore,
      FROM (
      SELECT 
        host, 
        AVG(CLS) AS CLS,
        AVG(LCP) AS LCP,
        AVG(FID) AS FID,
        AVG(load) AS load,
        AVG(click) AS click,
        COUNTIF(chain = 1) AS reach,
        COUNTIF(chain = 7) AS persistence
      FROM chains 
      WHERE (host = @domain OR @domain = "-") 
        AND host IS NOT NULL
        AND host NOT LIKE "%.hlx.%"
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