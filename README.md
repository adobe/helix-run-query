# Helix Run Query
> A service that runs premade queries on datasets created by Helix-Logging
## Status
[![codecov](https://img.shields.io/codecov/c/github/adobe/helix-run-query.svg)](https://codecov.io/gh/adobe/helix-run-query)
[![CircleCI](https://img.shields.io/circleci/project/github/adobe/helix-run-query.svg)](https://circleci.com/gh/adobe/helix-run-query)
[![GitHub license](https://img.shields.io/github/license/adobe/helix-run-query.svg)](https://github.com/adobe/helix-run-query/blob/master/LICENSE.txt)
[![GitHub issues](https://img.shields.io/github/issues/adobe/helix-run-query.svg)](https://github.com/adobe/helix-run-query/issues)
[![LGTM Code Quality Grade: JavaScript](https://img.shields.io/lgtm/grade/javascript/g/adobe/helix-run-query.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/adobe/helix-run-query)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release) [![Greenkeeper badge](https://badges.greenkeeper.io/adobe/helix-run-query.svg)](https://greenkeeper.io/)

## Installation
run ```npm install``` in root of repository

## Usage
Assuming you are using httpie as your http client:

```bash
http -f POST https://adobeioruntime.net/api/v1/web/helix/helix-services/run-query@v1/next-resource?limit=20 

```

Parameters needed in request:
1. GOOGLE_PROJECT_ID,
2. GOOGLE_PRIVATE_KEY,
3. GOOGLE_CLIENT_EMAIL,
4. token XOR X-Token,
5. service XOR X-Service,
6. Any google or helix parameters that are placeholders in the query file;

For more, see the [API documentation](docs/API.md).

## Development

### Queries

    Query files live in the src/queries directory. They are static resources; that are loaded into run-query and
    then sent to Bigquery for actual execution. It is up to the developer to ensure their query is correct; this 
    can be done by using the Bigquery console. 

    Once a query file is complete and correct, you may add it as a static resource; so that it won't be excluded during openwhisk deployment. 
    In the root of the repository; find the package.json and add your query file (file with .sql extension) under static: 

    "wsk": {
    "name": "helix-services/run-query@${version}",
        "static": [
            "src/queries/next-resource.sql"
        ]
    },

    Now, query file can be executed as an action; triggered by a request as such: 

    http -f POST https://adobeioruntime.net/api/v1/web/helix/helix-services/run-query@v1/SOME-QUERY-WITHOUT-EXTENSION?limit=20 param1=value1 param2=value2

### Parameterized Queries

    Helix Run Query provides query developers the ability to specify parameters anywhere in their queries. 
    Using  ^param anywhere in the query; and providing a corresponding {param: 'value'} in the request; enables 
    you to effectively parameterize just about any part of the query. It is not advised to parameterize anything beyond 
    table name or limit. Anything else; can make your query susceptible to SQL injection. 

## Deploying Helix Run Query

Deploying Helix Run Query requires the `wsk` command line client, authenticated to a namespace of your choice. For Project Helix, we use the `helix` namespace.

All commits to master that pass the testing will be deployed automatically. All commits to branches that will pass the testing will get commited as `/helix-services/run-query@ci<num>` and tagged with the CI build number.
