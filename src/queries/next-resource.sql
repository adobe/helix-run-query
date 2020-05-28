/*
 * Copyright 2020 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
*/
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