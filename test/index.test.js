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

const NodeHttpAdapter = require('@pollyjs/adapter-node-http');
const FSPersister = require('@pollyjs/persister-fs');
const { setupMocha: setupPolly } = require('@pollyjs/core');
const env = require('../src/env.js');
const { cleanRequestParams, cleanQueryParams } = require('../src/util.js');

describe('Index Tests', async () => {
  const goodQuery = 'select * from requests';

  const goodExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQuery } });

  const badAuthIndex = proxyquire('../src/index.js', {
    './sendquery.js': goodExec,
    './util.js': {
      authFastly: () => {
        throw new Error('Authentication Error with Fastly');
      },
    },
  }).main;

  const index = proxyquire('../src/index.js', { './sendquery.js': goodExec, './util.js': { authFastly: () => true } }).main;

  const service = 'fake_name';

  setupPolly({
    recordFailedRequests: false,
    recordIfMissing: false,
    matchRequestsBy: {
      headers: {
        exclude: ['authorization', 'user-agent'],
      },
      body: false,
      url: false,
      order: false,
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
    )
      .passthrough();
  });

  it('index function is present', async () => {
    await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 10,
      service,
    });
  });

  it('index function returns an object', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 10,
      service,
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 10);
  });


  it('index function truncates long responses', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 20000,
      service,
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(result.body.truncated);
  });

  it('index function returns 500 on error', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: 'env.key',
      token: env.token,
      __ow_path: 'list-everything',
      service,
      limit: 10,
    });
    assert.equal(typeof result, 'object');
    assert.equal(result.statusCode, 500);
  });

  it('index function returns 401 on auth error', async () => {
    const result = await badAuthIndex({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: 'env.key',
      token: 'notatoken',
      __ow_path: 'list-everything',
      service,
      limit: 10,
    });
    assert.equal(typeof result, 'object');
    assert.equal(result.statusCode, 401);
  });

  it('index function returns an object with ow_headers', async () => {
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
      limit: 10,
      service: 'Wrong Service',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 10);
  });

  it('index function returns an object with ow_headers', async () => {
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
      limit: 10,
      service: 'Wrong Service',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 10);
  });
});

describe('testing cleanRequestParams', () => {
  it('cleanRequestParams returns object', () => {
    const result = cleanRequestParams({});
    assert.equal(typeof result, 'object');
    assert.ok(!Array.isArray(result));
  });

  it('cleanRequestParams returns clean object', () => {
    const result = cleanRequestParams({
      FOOBAR: 'ahhhh',
      foobar: 'good',
      __foobar: 'bad',
    });
    assert.deepStrictEqual(result, {
      foobar: 'good',
    });
  });

  it('cleanQueryParams leaves only BigQuery', () => {
    const query = 'SELECT ^something1, ^something2 WHERE ^tablename and @bqParam';

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
      something2: '"Lucy',
      something3: 'foobar',
      bqParam: 'Google BigQuery Parameter',
    };
    const result = cleanQueryParams(query, params);

    assert.deepStrictEqual(result, {
      bqParam: 'Google BigQuery Parameter',
    });
  });
});
