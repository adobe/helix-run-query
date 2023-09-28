# Helix Run Query
> A service that runs premade queries on datasets created by Helix-Logging
## Status
[![codecov](https://img.shields.io/codecov/c/github/adobe/helix-run-query.svg)](https://codecov.io/gh/adobe/helix-run-query)
[![CircleCI](https://img.shields.io/circleci/project/github/adobe/helix-run-query.svg)](https://circleci.com/gh/adobe/helix-run-query)
[![GitHub license](https://img.shields.io/github/license/adobe/helix-run-query.svg)](https://github.com/adobe/helix-run-query/blob/main/LICENSE.txt)
[![GitHub issues](https://img.shields.io/github/issues/adobe/helix-run-query.svg)](https://github.com/adobe/helix-run-query/issues)
[![LGTM Code Quality Grade: JavaScript](https://img.shields.io/lgtm/grade/javascript/g/adobe/helix-run-query.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/adobe/helix-run-query)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release) 

## Usage
Assuming you are using `curl` as your http client:

```bash
curl https://helix-pages.anywhere.run/helix-services/run-query@v3/rum-dashboard
```

```json
{
    "results": {
      "data": [
        {
            "req_url": "https://helix-secret.fake",
            "resp_http_Content_Type": "text/html; charset=UTF-8",
            "status_code": "404",
            "visits": 1
        }
      ],
    }
}
```

The data is returned in Franklin's Spreadsheet Object Notation (SSHON) format. For more information, see [Franklin's documentation](https://www.hlx.live/developer/spreadsheets). There are two sheets, one called `meta` with information about the query and the other called `results` with the actual data.

### Authentication

The service is protected by shared secrets that apply to a domain and all subdomains. These secrets are called domain keys.

You can pass the domain key in two ways:
- as a query parameter `?domainkey=…`
- as an `Authorization: Bearer …` header

Domain keys can be read-only or read-write. Read-write domain keys allow you
to issue new domain keys and revoke exiting keys. They should never be passed
to the service in a GET request.

It is best practice to use read-write domain keys only to issue new read-only
keys and work with those.

### Query Parameters

Each query can define a set of query parameters, but the following parameters are
supported almost universally:

- `limit` - the maximum number of rows to return
- `interval` – the number of days of data to return
- `offset` – the number of rows or days to skip
- `startdate` – the start date of the data to return (this will only take effect if `interval` is not set)
- `enddate` – the end date of the data to return (this will only take effect if `interval` is not set)
- `timezone` – the timezone to use for the dates (defaults to `UTC`)
- `url` – the URL to filter the data by. By default, the URL is taken as a partial filter, so `url=https://www.adobe.com/` will include `https://www.adobe.com/`, `https://www.adobe.com/?campaign=exapmle`, and `https://www.adobe.com/en/`. If you want to filter by an exact URL, use `url=https://www.adobe.com/$`. If you want to filter by an exact URL, but allow URL parameters, use `url=https://www.adobe.com/?`
- `checkpoint` – filter results by a particular checkpoint. The list of common checkpoints can be found [here](https://www.hlx.live/developer/rum#checkpoints).
- `device` – filter results by a particular class. Possible values are `all` (default), `desktop`, `mobile`, and `bot`.
- `source` – filter results by a particular source parameter value

# Deprecation Notice

`helix-run-query@v2` is deprecated and will be removed in the future. Please use `helix-run-query@v3` instead.

## Changes from v2 to v3

- the output format has changed from a custom format to Franklin's Spreadsheet Object Notation (SSHON)
- the `domain` parameter has been removed and replaced with `url`
- the `url` parameter now supports partial matching and exact matching

For a full list of changes, please see the [CHANGELOG](CHANGELOG.md#300-2023-04-21).

# Development

## `dev.sh` script

Running queries in development using the `bq` tool can be cumbersome, as all parameters
in the query are required. The `dev.sh` script can be used to run queries in development
mode and will fill out important parameters with defaut values.

Start setting it up by exporting your domain key to `DOMAINKEY`, then run `dev.sh <query>`.

```bash
$ export DOMAINKEY=…
$ sh dev.sh rum-dashboard url www.adobe.com
```

You can pass additional parameters to the query as seen above. The second (optional) parameter allows you to specify the output format, with values `pretty`, `csv`, `json`
being supported.

## Required Environment Variables

This service depends on three external services to operate:

- Fastly
- Google Cloud Platform

It is configured using a number of environment variables that are required for testing (tests that miss required variables will be skipped) and deployment (deployment will fail or be non-functional). These variables are required and this is how to set them up:

### `GOOGLE_CLIENT_EMAIL`

This is the email address associated with a Google Cloud Platform Service account. It looks like `<name>@<project>.iam.gserviceaccount.com`. You can create a proper service account following [the instructions in the Google Cloud Platform documentation](https://cloud.google.com/iam/docs/creating-managing-service-accounts) or this step-by-step guide:

1. Log in to [Google Cloud Platform Console](https://console.cloud.google.com)
2. Select menu → "IAM & admin" → "Service accounts" → "Create service account"
3. Create the service account
4. Add the following roles to the service account:
   * BigQuery Admin
   * Service Account Admin
   * Service Account Key Admin
   * Service Account Key Admin
5. Create a private key in JSON format for the service account and download the key file

**Note:** The private key file and the value of the `GOOGLE_CLIENT_EMAIL` environment variable should be considered private and should never be checked in to source control.

The downloaded file will look something like this:

```json
{
  "type": "service_account",
  "project_id": "project-12345678",
  "private_key_id": "111122223333aaaabbbbccccdddd123412345",
  "private_key": "-----BEGIN PRIVATE KEY-----\n…\n-----END PRIVATE KEY-----\n",
  "client_email": "example-account@project-12345678.iam.gserviceaccount.com",
  "client_id": "111122223333444456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/example-account%40project-12345678.iam.gserviceaccount.com"
}
```

Copy the value of the `client_email` field (e.g. `example-account@project-12345678.iam.gserviceaccount.com`) and save it in the `GOOGLE_CLIENT_EMAIL` environment variable.

### `GOOGLE_PRIVATE_KEY`

This is the private key associated with the Google Cloud Platform Service account created above. In order to retrieve the correct value, see [Creating and Managing Service Account Keys in the Google Cloud Platform documentation](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) or continue the step-by-step guide from above:

6. Make sure you've followed all steps to get the value of `GOOGLE_CLIENT_EMAIL`
7. Copy the value of the `private_key` property in the JSON file you've downloaded

**Note:** The private key and the value of the `GOOGLE_PRIVATE_KEY` environment variable should be considered private and should never be checked in to source control.

The private key is a multi-line value.

**Note:** Private keys created using an API typically have a short expiration time and need to be rotated in regular intervals. Even for private keys that have been created manually, regular rotation is a best practice.

### `GOOGLE_PROJECT_ID`

This is the Google Cloud Platform project ID. It looks like `project-12345678` and you will find it in lots of places in the Google Cloud Platform Console UI. In addition, you can just take the value of the `project_id` property in your downloaded key JSON file.

### `HLX_FASTLY_NAMESPACE`

This property is only required for testing and development. It is the service config ID that you can retrieve from Fastly.

For testing, it is a good idea to use a separate, non-production service config, as the tests not only perform frequent updates, but they also rotate the private keys of the created Google Cloud Platform service accounts. As the tests don't activate the service config, this will lead to an invalid logging configuration in a short time.

### `HLX_FASTLY_AUTH`

This property is only required for testing and development. It is an API token for the Fastly API. Follow the [instructions in the Fastly documentation](https://docs.fastly.com/guides/account-management-and-security/using-api-tokens) to create a token.

The token needs to have `global`, i.e. write access to your service config.


**Note:** The API token and the value of the `HLX_FASTLY_AUTH` environment variable should be considered private and should never be checked in to source control.
For more, see the [API documentation](docs/API.md).

## Development

You need node>=8.0.0 and npm>=5.4.0. Follow the typical npm install, npm test workflow.

Contributions are highly welcome.

### Queries

Query files live in the `src/queries` directory, although they can also live in subdirectories of `src/queries`.
They are static resources; that are loaded into `run-query` and then sent to BigQuery for actual execution. In order to be bundled into the lambda function, they have to be listed in `package.json`
It is up to the developer to ensure their query is correct; this can be done by using the BigQuery console.

The build process includes a `sqlfluff` check.  The recommendation is to install `sqlfluff` locally so that you
can fix any syntax issues prior to commit. See https://docs.sqlfluff.com/en/stable/gettingstarted.html.

Once a query file is complete and correct, you may add it as a static resource; so that it won't be excluded during OpenWhisk deployment.
In the root of the repository; find the package.json and add your query file (file with `.sql` extension) under `static`: 

```json
"wsk": {
"name": "helix-services/run-query@${version}",
"static": [
     "src/queries/next-resource.sql"
  ]
},
```

Now, query file can be executed as an action; triggered by a request as such: 

```bash
curl -X POST -H "Content-Type: application/json" https://adobeioruntime.net/api/v1/w/helix-services/run-query@v1/next-resource -d '{"service":"secretService", "token":"secretToken", "queryParam1":"value"}'
```

### Parameterized Queries

Helix Run Query provides query developers the ability to specify parameters anywhere in their queries. 
Using  `^param` anywhere in the query; and providing a corresponding `{param: 'value'}` in the request; enables 
you to effectively parameterize just about any part of the query. It is not advised to parameterize anything beyond 
table name or limit. Anything else; can make your query susceptible to SQL injection. 

## Deploying Helix Run Query

Deploying Helix Run Query requires the `wsk` command line client, authenticated to a namespace of your choice. For Project Helix, we use the `helix` namespace.

All commits to main that pass the testing will be deployed automatically.
All commits to branches that will pass the testing will get commited as `/helix-services/run-query@ci<num>` and tagged with the CI build number.
The CI build number can be found next to the `branch-deploy` job for the pipeline execution related to a commit at https://app.circleci.com/pipelines/github/adobe/helix-run-query.

For a query to be available at `helix-services/run-query@v3` (semantic versioning), a commit in the pull request must follow the message convention standards defined at https://www.conventionalcommits.org/en/v1.0.0/.
