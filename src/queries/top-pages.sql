--- description: most requested sites by Helix.
--- Authorization: fastly
--- Cache-Control: max-age=1296000
--- fromDays: 30
--- toDays: 0
--- limit: 10
SELECT * FROM (
  SELECT
    req_url,
    COUNT(req_http_x_cdn_request_id) AS reqs
  FROM (
    ^allrequests(
        resp_http_content_type,
        status_code,
        time_start_usec,
        req_url,
        req_http_x_cdn_request_id
      )
    WHERE
      _table_suffix BETWEEN
      FORMAT_TIMESTAMP(
        "%Y%m",
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)
      ) AND
      format_timestamp(
        "%Y%m",
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)
      )
  )
  WHERE
    resp_http_content_type LIKE "text/html%"
    AND status_code = "200" AND
    time_start_usec > CAST(
      UNIX_MICROS(
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)
      ) AS STRING
    ) AND
    time_start_usec < CAST(
      UNIX_MICROS(
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)
      ) AS STRING
    )
  GROUP BY
    req_url
  ORDER BY reqs DESC
)
WHERE reqs > 10
LIMIT @limit
