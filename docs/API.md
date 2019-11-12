## Functions

<dl>
<dt><a href="#execute">execute(email, key, project, query, service, params)</a></dt>
<dd><p>executes a query using Google Bigquery API</p>
</dd>
<dt><a href="#authFastly">authFastly(token, service)</a></dt>
<dd><p>authenticates token and service with Fastly</p>
</dd>
<dt><a href="#loadQuery">loadQuery(query)</a></dt>
<dd><p>reads a query file and loads it into memory</p>
</dd>
<dt><a href="#cleanQueryParams">cleanQueryParams(query, params)</a></dt>
<dd><p>strips params object of helix query parameters</p>
</dd>
<dt><a href="#queryReplace">queryReplace(query, params)</a></dt>
<dd><p>replaces helix query parameter placeholders denoted by ^param
in a query with value in param object. Example; SELECT * FROM ^tablename
^tabename is the query parameter; so it&#39;s expected that the params object
contains {tablename: &#39;some-value&#39;}</p>
</dd>
<dt><a href="#getExtraParameters">getExtraParameters(query)</a></dt>
<dd><p>processes additional parameters to be passed into request
for example; --- Cache-Control: max-age: 300.</p>
</dd>
<dt><a href="#cleanRequestParams">cleanRequestParams(params)</a></dt>
<dd><p>removes used up parameters from request</p>
</dd>
</dl>

<a name="execute"></a>

## execute(email, key, project, query, service, params)
executes a query using Google Bigquery API

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| email | <code>string</code> | email address of the Google service account |
| key | <code>string</code> | private key of the global Google service account |
| project | <code>string</code> | the Google project ID |
| query | <code>string</code> | the name of a .sql file in queries directory |
| service | <code>string</code> | the serviceid of the published site |
| params | <code>object</code> | parameters for substitution into query |

<a name="authFastly"></a>

## authFastly(token, service)
authenticates token and service with Fastly

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| token | <code>string</code> | Fastly Authentication Token |
| service | <code>string</code> | serviceid for a helix-project |

<a name="loadQuery"></a>

## loadQuery(query)
reads a query file and loads it into memory

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| query | <code>string</code> | name of the query file |

<a name="cleanQueryParams"></a>

## cleanQueryParams(query, params)
strips params object of helix query parameters

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| query | <code>string</code> | the content read from a query file |
| params | <code>object</code> | query parameters, that are inserted into query |

<a name="queryReplace"></a>

## queryReplace(query, params)
replaces helix query parameter placeholders denoted by ^param
in a query with value in param object. Example; SELECT * FROM ^tablename
^tabename is the query parameter; so it's expected that the params object
contains {tablename: 'some-value'}

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| query | <code>string</code> | the content read from a query file |
| params | <code>\*</code> | query parameters, that are inserted into query |

<a name="getExtraParameters"></a>

## getExtraParameters(query)
processes additional parameters to be passed into request
for example; --- Cache-Control: max-age: 300.

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| query | <code>string</code> | the content read from a query file |

<a name="cleanRequestParams"></a>

## cleanRequestParams(params)
removes used up parameters from request

**Kind**: global function  

| Param | Type | Description |
| --- | --- | --- |
| params | <code>object</code> | all parameters contained in a request |

