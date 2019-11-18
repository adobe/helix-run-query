SELECT * FROM (
  SELECT 
    req_url, count(req_http_X_CDN_Request_ID) as reqs
  FROM (
    # SELECT * FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*` UNION ALL # ^myrequests
    # SELECT * FROM `helix_logging_0gDEZCgYFsF773b88iAgvS.requests*` UNION ALL
    # SELECT * FROM `helix_logging_0NhqbaDRAZzS9spG6wI8og.requests*`
    ^allrequests
    WHERE
        # only read the table shards for the last two months (coarse grained date filtering, makes queries cheap)
    _TABLE_SUFFIX BETWEEN 
      FORMAT_TIMESTAMP("%Y%m" , TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AND 
      FORMAT_TIMESTAMP("%Y%m" , TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY))
    )
  WHERE 
    resp_http_Content_Type LIKE "text/html%" AND
    status_code = "200" AND
    # only consider requests happening in the last 30 days (fine-grained date filtering, makes queries accurate)
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS STRING)
  GROUP BY
    req_url
  ORDER BY reqs DESC
)
WHERE reqs > 10
LIMIT 1000