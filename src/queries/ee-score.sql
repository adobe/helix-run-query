--- description: Show content reach and persistence over time
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 60
--- offset: 0
--- domain: -
--- range: 5
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
  CHR(64 + CAST(CEIL(score) AS INT64))
);

WITH visits AS (
  SELECT
    id,
    REGEXP_REPLACE(ANY_VALUE(url), "\\?.*$", "") AS url,
    ANY_VALUE(hostname) AS host,
    TIMESTAMP_TRUNC(MAX(time), DAY) AS visittime,
    MAX(weight) AS weight,
    MAX(lcp) AS lcp,
    MAX(cls) AS cls,
    MAX(fid) AS fid,
    MAX(IF(checkpoint = "top", 1, 0)) AS top,
    MAX(IF(checkpoint = "load", 1, 0)) AS load,
    MAX(IF(checkpoint = "click", 1, 0)) AS click,
    TIMESTAMP_DIFF(MAX(time), MIN(time), SECOND) AS engagementduration
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
    visittime,
    url,
    MAX(host) AS host,
    COUNT(id) AS events,
    SUM(weight) AS visits,
    AVG(lcp) AS lcp,
    AVG(cls) AS cls,
    AVG(fid) AS fid,
    LEAST(IF(SUM(top) > 0, SUM(load) / SUM(top), 0), 1) AS load,
    LEAST(IF(SUM(top) > 0, SUM(click) / SUM(top), 0), 1) AS click,
    APPROX_QUANTILES(engagementduration, 100)[OFFSET(50)] AS engagementduration,
    APPROX_QUANTILES(
      engagementduration, 100
    )[OFFSET(50)] * SUM(weight) AS cumulativeengagement
  FROM visits # FULL JOIN days ON (days.visittime = visits.visittime)
  GROUP BY visittime, url
),

steps AS (
  SELECT
    visittime,
    url,
    host,
    events,
    visits,
    lcp,
    cls,
    fid,
    load,
    click,
    engagementduration,
    cumulativeengagement,
    TIMESTAMP_DIFF(
      visittime, LAG(visittime) OVER(PARTITION BY url ORDER BY visittime), DAY
    ) AS step
  FROM urldays
),

chains AS (
  SELECT
    visittime,
    url,
    host,
    events,
    visits,
    lcp,
    cls,
    fid,
    load,
    click,
    engagementduration,
    cumulativeengagement,
    step,
    COUNTIF(step = 1) OVER(PARTITION BY url ORDER BY visittime) AS chainlength
  FROM steps
),

# urlchains AS (
#  SELECT
#    url,
#    time,
#    chain,
#    events,
#    visits
#  FROM chains
#  ORDER BY chain DESC
# ),
#
# powercurve AS (
#  SELECT
#    MAX(chain) AS persistence,
#    COUNT(url) AS reach
#  FROM urlchains
#  GROUP BY chain
#  ORDER BY MAX(chain) ASC
#  LIMIT 31 OFFSET 1
# ),

powercurvequintiles AS (
  SELECT
    APPROX_QUANTILES(DISTINCT reach, @range) AS reach,
    APPROX_QUANTILES(DISTINCT persistence, @range) AS persistence
  FROM (
    SELECT * FROM (
      SELECT
        host,
        COUNTIF(chainlength = 1) AS reach,
        COUNTIF(chainlength = 7) AS persistence
      FROM chains
      GROUP BY host
    )
    WHERE reach > 0 AND persistence > 0 AND host IS NOT NULL
  )
),

cwvquintiles AS (
  SELECT
    APPROX_QUANTILES(DISTINCT lcp, @range) AS lcp,
    APPROX_QUANTILES(DISTINCT cls, @range) AS cls,
    APPROX_QUANTILES(DISTINCT fid, @range) AS fid,
    APPROX_QUANTILES(DISTINCT load, @range) AS load,
    APPROX_QUANTILES(DISTINCT click, @range) AS click,
    APPROX_QUANTILES(DISTINCT engagementduration, @range) AS engagementduration,
    APPROX_QUANTILES(
      DISTINCT cumulativeengagement, @range
    ) AS cumulativeengagement
  FROM chains
),

cwvquintiletable AS (
  SELECT
    num,
    lcp[OFFSET(num)] AS lcp,
    cls[OFFSET(num)] AS cls,
    fid[OFFSET(num)] AS fid,
    load[OFFSET(@range - num)] AS load,
    click[OFFSET(@range - num)] AS click,
    engagementduration[OFFSET(num)] AS engagementduration,
    cumulativeengagement[OFFSET(num)] AS cumulativeengagement
  FROM cwvquintiles INNER JOIN UNNEST(GENERATE_ARRAY(0, @range)) AS num
),

powercurvequintiletable AS (
  SELECT
    num,
    reach[OFFSET(@range - num)] AS reach,
    persistence[OFFSET(@range - num)] AS persistence
  FROM powercurvequintiles INNER JOIN UNNEST(GENERATE_ARRAY(0, @range)) AS num
),

quintiletable AS (
  # SELECT * FROM powercurve
  SELECT
    cwvquintiletable.cls AS cls,
    cwvquintiletable.lcp AS lcp,
    cwvquintiletable.fid AS fid,
    cwvquintiletable.load AS load,
    cwvquintiletable.click AS click,
    cwvquintiletable.engagementduration AS engagementduration,
    cwvquintiletable.cumulativeengagement AS cumulativeengagement,
    powercurvequintiletable.reach AS reach,
    powercurvequintiletable.persistence AS persistence,
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
        ) + ((reachscore + persistencescore + loadscore) / 3)
        + (
          (clickscore + engagementdurationscore + cumulativeengagementscore) / 3
        )
      ) / 3
    ) AS experiencescore,
    LABELSCORE((clsscore + lcpscore + fidscore) / 3) AS perfscore,
    LABELSCORE(
      (reachscore + persistencescore + loadscore) / 3
    ) AS audiencescore,
    LABELSCORE(
      (clickscore + engagementdurationscore + cumulativeengagementscore) / 3
    ) AS engagementscore
  FROM (
    SELECT
      host,
      chained.cls AS cls,
      # (SELECT num FROM quintiletable WHERE quintiletable.CLS <= chained.CLS) AS clsscore,
      chained.lcp AS lcp,
      chained.fid AS fid,
      chained.load AS load,
      chained.click AS click,
      chained.engagementduration AS engagementduration,
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
        SELECT MAX(num)
        FROM
          (
            SELECT num
            FROM
              quintiletable
            WHERE quintiletable.engagementduration <= chained.engagementduration
          )
      ) AS engagementdurationscore,
      (
        SELECT MAX(num)
        FROM
          (
            SELECT num
            FROM
              quintiletable
            WHERE
              quintiletable.cumulativeengagement <= chained.cumulativeengagement
          )
      ) AS cumulativeengagementscore,
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
          AVG(engagementduration) AS engagementduration,
          AVG(cumulativeengagement) AS cumulativeengagement,
          COUNTIF(chainlength = 1) AS reach,
          COUNTIF(chainlength = 7) AS persistence
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
    AND engagementdurationscore IS NOT NULL
    AND cumulativeengagementscore IS NOT NULL
  )
)

#SELECT MIN(num) FROM (SELECT * FROM quintiletable WHERE 0.9841262752758466 >= quintiletable.click)
SELECT
  host,
  experiencescore,
  perfscore,
  audiencescore,
  engagementscore
FROM lookmeup
ORDER BY REGEXP_EXTRACT(host, r"\..*") ASC, host ASC
