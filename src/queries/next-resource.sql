SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
FROM ^tablename
WHERE 
  resp_http_Content_Type LIKE "text/html%" AND
  status_code LIKE "404"
GROUP BY
  req_url, resp_http_Content_Type, status_code 
ORDER BY visits DESC
LIMIT @limit