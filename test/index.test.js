/*
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

/* eslint-env mocha */

'use strict';

const assert = require('assert');
const proxyquire = require('proxyquire');
const path = require('path');
const { Request } = require('@adobe/helix-fetch');

const NodeHttpAdapter = require('@pollyjs/adapter-node-http');
const FSPersister = require('@pollyjs/persister-fs');
const { setupMocha: setupPolly } = require('@pollyjs/core');
const env = require('../src/env.js');

describe('Index Tests', async () => {
  const goodQueryWithAuth = '--- description: some fake comments that mean nothing\n--- Vary2: X-Token, X-Service\n--- Authorization: fastly\n--- Cache-Control: max-age=300\nselect * from requests LIMIT @limit';

  const goodExecWithAuth = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQueryWithAuth, authFastly: () => true } });
  const execWithBadAuth = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQueryWithAuth, authFastly: () => Promise.reject(new Error('Failed')) } });

  const index = proxyquire('../src/index.js', { './sendquery.js': goodExecWithAuth }).main;
  const badIndex = proxyquire('../src/index.js', { './sendquery.js': execWithBadAuth }).main;

  const service = 'fake_name';

  setupPolly({
    recordFailedRequests: false,
    recordIfMissing: false,
    matchRequestsBy: {
      headers: {
        exclude: ['authorization', 'user-agent', 'x-goog-api-client'],
      },
      body: true,
      url: false,
      order: false,
      method: true,
    },
    logging: false,
    adapters: [NodeHttpAdapter],
    persister: FSPersister,
    persisterOptions: {
      fs: {
        recordingsDir: path.resolve(__dirname, 'fixtures/recordings'),
      },
    },
  });

  beforeEach(function first() {
    const { server } = this.polly;
    server.any(
      [
        'https://www.googleapis.com/oauth2/v4/token',
        'https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/jobs',
        'https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/datasets/helix_logging_fake_name',
      ],
    ).passthrough();

    server.any('https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/queries/*')
      .on('beforePersist', (req, recording) => {
        // eslint-disable-next-line no-param-reassign
        recording.request.headers['primary-key'] = 'Helix-Key';
      })
      .on('request', (req) => {
        req.headers['primary-key'] = 'Helix-Key';
      });
  });

  it('index function is present', async () => {
    await index(new Request('https://helix-run-query.com/list-everything?limit=3', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: env.key,
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });
  });

  it('index function returns an object', async () => {
    const response = await index(new Request('https://helix-run-query.com/list-everything?limit=3', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: env.key,
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });
    const body = await response.json();

    assert.equal(typeof body, 'object');
    assert.ok(Array.isArray(body.results));
    assert.ok(!body.truncated);
    assert.equal(body.results.length, 3);
    assert.equal(response.headers.get('content-type'), 'application/json');
    assert.equal(response.headers.get('vary'), 'X-Token, X-Service');
    assert.equal(response.headers.get('vary2'), 'X-Token, X-Service');
    assert.equal(response.headers.get('cache-control'), 'max-age=300');

    assert.deepEqual(body.requestParams, { limit: 3 });
    assert.equal(body.description, 'some fake comments that mean nothing');
  });

  it('index function truncates long responses', async () => {
    const response = await index(new Request('https://helix-run-query.com/list-everything?limit=2000', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: env.key,
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });
    const body = await response.json();

    assert.equal(typeof body, 'object');
    assert.ok(Array.isArray(body.results));
    assert.ok(body.truncated);
    assert.notEqual(body.results.length, 2000);
    assert.equal(response.headers.get('content-type'), 'application/json');
    assert.equal(response.headers.get('vary'), 'X-Token, X-Service');
    assert.equal(response.headers.get('vary2'), 'X-Token, X-Service');
    assert.equal(response.headers.get('cache-control'), 'max-age=300');

    assert.deepEqual(body.requestParams, { limit: 2000 });
    assert.equal(body.description, 'some fake comments that mean nothing');
  }).timeout(10000);

  it('index function returns 500 on error', async () => {
    const response = await index(new Request('https://helix-run-query.com/list-everything?limit=3', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: 'env.key',
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });
    assert.equal(typeof response, 'object');
    assert.equal(response.status, 500);
  });

  it('index function returns 401 on auth error', async () => {
    const response = await badIndex(new Request('https://helix-run-query.com/list-everything?limit=3', {
      headers: {
        'x-service': service,
        'x-token': 'notatoken',
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: 'env.key',
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });
    assert.equal(typeof response, 'object');
    assert.equal(response.status, 401);
  });

  it.skip('index function returns an object with ow_headers', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      __ow_headers: {
        'x-token': env.token,
        'x-service': service,
      },
      token: 'Wrong Token',
      __ow_path: 'list-everything',
      limit: 3,
      service: 'Wrong Service',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 3);
    assert.deepEqual(result.headers, {
      'content-type': 'application/json',
      Vary: 'X-Token, X-Service',
      Vary2: 'X-Token, X-Service',
      'Cache-Control': 'max-age=300',
    });
    assert.deepEqual(result.body.requestParams, { limit: 3 });
    assert.equal(result.body.description, 'some fake comments that mean nothing');
  });

  it.skip('index function returns an object with ow_headers', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      __ow_headers: {
        'x-token': env.token,
        'x-service': service,
      },
      token: 'Wrong Token',
      __ow_path: 'list-everything',
      limit: 3,
      service: 'Wrong Service',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 3);
    assert.deepEqual(result.headers, {
      'content-type': 'application/json',
      Vary: 'X-Token, X-Service',
      Vary2: 'X-Token, X-Service',
      'Cache-Control': 'max-age=300',
    });
    assert.deepEqual(result.body.requestParams, { limit: 3 });
    assert.equal(result.body.description, 'some fake comments that mean nothing');
  });

  it('index returns query metadata if path ends with .txt, .html', async () => {
    const response = await index(new Request('https://helix-run-query.com/list-everything.txt?limit=3', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: env.key,
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });

    assert.equal(response.status, 200);
    assert.equal(await response.text(), `some fake comments that mean nothing
  * limit: 3

`);
    assert.equal(response.headers.get('content-type'), 'text/plain; charset=utf-8');
    assert.equal(response.headers.get('authorization'), 'fastly');
    assert.equal(response.headers.get('vary2'), 'X-Token, X-Service');
    assert.equal(response.headers.get('cache-control'), 'max-age=300');
  });

  it('index returns csv if path ends with .csv', async () => {
    const response = await index(new Request('https://helix-run-query.com/list-everything.csv?limit=3', {
      headers: {
        'x-service': service,
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: env.email,
        GOOGLE_PRIVATE_KEY: env.key,
        GOOGLE_PROJECT_ID: env.projectid,
      },
    });

    const text = await response.text();
    assert.equal(response.status, 200, text);
    assert.equal(text.split('\n').length, 4);
    assert.equal(text.split('\n')[0], 'client_geo_city,client_as_name,client_geo_conn_speed,client_geo_continent_code,client_geo_country_code,client_geo_gmt_offset,client_geo_latitude,client_geo_longitude,client_geo_metro_code,client_geo_postal_code,client_geo_region,client_ip_hashed,client_ip_masked,fastly_info_state,req_http_X_Ref,req_http_X_Repo,req_http_X_Static,req_http_X_Strain,req_http_X_Owner,server_datacenter,server_region,req_http_host,req_http_X_Host,req_url,req_http_X_URL,req_http_X_CDN_Request_ID,vcl_sub,time_start_usec,time_end_usec,time_elapsed_usec,resp_http_x_openwhisk_activation_id,resp_http_X_Version,req_http_Referer,req_http_User_Agent,resp_http_Content_Type,service_config,status_code');
    assert.equal(response.headers.get('content-type'), 'text/csv');
  });
});
