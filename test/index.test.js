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
const { cleanRequestParams, cleanQueryParams } = require('../src/util.js');
const env = require('../src/env.js');

describe('Index Tests', async () => {
  const goodQuery = 'select * from requests201905';
  const goodExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQuery } });

  const index = proxyquire('../src/index.js', { './sendquery.js': goodExec }).main;

  it('index function is present', async () => {
    await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 10,
      service: '0bxMEaYAJV6SoqFlbZ2n1f',
    });
  }).timeout(5000);

  it('index function returns an object', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 10,
      service: '0bxMEaYAJV6SoqFlbZ2n1f',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(!result.body.truncated);
    assert.equal(result.body.results.length, 10);
  }).timeout(5000);


  it('index function truncates long responses', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      token: env.token,
      __ow_path: 'list-everything',
      limit: 10000000,
      service: '0bxMEaYAJV6SoqFlbZ2n1f',
    });
    assert.equal(typeof result, 'object');
    assert.ok(Array.isArray(result.body.results));
    assert.ok(result.body.truncated);
  }).timeout(5000);

  it('index function returns 500 on error', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: 'env.key',
      token: env.token,
      __ow_path: 'list-everything',
      service: '0bxMEaYAJV6SoqFlbZ2n1f',
      limit: 10,
    });
    assert.equal(typeof result, 'object');
    assert.equal(result.statusCode, 500);
  });
  it('index function returns 401 on auth error', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: 'env.key',
      token: 'notatoken',
      __ow_path: 'list-everything',
      service: '0bxMEaYAJV6SoqFlbZ2n1f',
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
        'x-service': '0bxMEaYAJV6SoqFlbZ2n1f',
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
  }).timeout(5000);

  it('index function returns an object with ow_headers', async () => {
    const result = await index({
      GOOGLE_CLIENT_EMAIL: env.email,
      GOOGLE_PRIVATE_KEY: env.key,
      GOOGLE_PROJECT_ID: env.projectid,
      __ow_headers: {
        'x-token': env.token,
        'x-service': '0bxMEaYAJV6SoqFlbZ2n1f',
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
  }).timeout(5000);
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
