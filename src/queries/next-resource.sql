--- Authorization: none
--- Cache-Control: max-age=300
--- limit: 100
SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
FROM ( 
  ^myrequests
)
WHERE 
  resp_http_Content_Type LIKE "text/html%" AND
  status_code LIKE "200"
GROUP BY
  req_url, resp_http_Content_Type, status_code 
ORDER BY visits DESC
LIMIT @limit