--- Authorization: none
--- Cache-Control: max-age=300
--- fromDays: 30
--- toDays: 0
--- limit: 10
SELECT * FROM (
  SELECT 
    req_url, count(req_http_X_CDN_Request_ID) as reqs
  FROM (
    ^allrequests
    WHERE
      _TABLE_SUFFIX BETWEEN 
      FORMAT_TIMESTAMP("%Y%m" , TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)) AND 
      FORMAT_TIMESTAMP("%Y%m" , TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY))
    )
  WHERE 
    resp_http_Content_Type LIKE "text/html%" AND
    status_code = "200" AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)) AS STRING)
  GROUP BY
    req_url
  ORDER BY reqs DESC
)
WHERE reqs > 10
LIMIT @limit