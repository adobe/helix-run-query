/*
 * Copyright 2024 Adobe. All rights reserved.
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

import assert from 'assert';
import { Request } from '@adobe/fetch';

import { main } from '../src/index.js';

describe('Test Content Requests', () => {
  it('rum-content-requests should provide monthly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-content-requests'
      + '?url=dxc.com'
      + '&granularity=30'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 2, 'expecting 2 entries, 1 for march 2024, 1 for april 2024');
    const expected = [
      {
        content_requests: 25160,
        cr_apicalls: 760,
        cr_margin_of_error: 3303,
        cr_pageviews: 24400,
        error404_requests: 0,
        hostname: 'dxc.com',
        html_requests: 28400,
        id: '0ac8dd00086a26594ab800acc5f79c21450ee147',
        json_requests: 4300,
        month: 3,
        time: '2024-03-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
      {
        content_requests: 18620,
        cr_apicalls: 520,
        cr_margin_of_error: 2765,
        cr_pageviews: 18100,
        error404_requests: 0,
        hostname: 'dxc.com',
        html_requests: 19900,
        id: 'c6dfddbcc0addf585931499895b1f0729d50f390',
        json_requests: 2900,
        month: 4,
        time: '2024-04-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-content-requests should provide yearly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-content-requests'
      + '?url=dxc.com'
      + '&granularity=365'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 1, 'expecting 1 entries, 1 for year 2024');
    const expected = [
      {
        content_requests: 43780,
        cr_apicalls: 1280,
        cr_margin_of_error: 6068,
        cr_pageviews: 42500,
        error404_requests: 0,
        hostname: 'dxc.com',
        html_requests: 48300,
        id: '3789fdb2d363d27da6a6dcffbe6d4b3d65aeaae5',
        json_requests: 7200,
        month: null,
        time: '2024-01-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-content-requests should provide data for multiple hosts', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-content-requests'
      + '?url=dxc.com,blog.adobe.com'
      + '&granularity=365'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 2, 'expecting 2 entries, 1 for each host for year 2024');
    const expected = [
      {
        content_requests: 188220,
        cr_apicalls: 99140,
        cr_margin_of_error: 8556,
        cr_pageviews: 89080,
        error404_requests: 1700,
        hostname: 'blog.adobe.com',
        html_requests: 94880,
        id: '3d8835af40a49938c772d724480e09a35aa9b881',
        json_requests: 508200,
        month: null,
        time: '2024-01-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
      {
        content_requests: 43780,
        cr_apicalls: 1280,
        cr_margin_of_error: 6068,
        cr_pageviews: 42500,
        error404_requests: 0,
        hostname: 'dxc.com',
        html_requests: 48300,
        id: '3789fdb2d363d27da6a6dcffbe6d4b3d65aeaae5',
        json_requests: 7200,
        month: null,
        time: '2024-01-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-content-requests should provide the second page of the monthly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-content-requests'
      + '?url=dxc.com'
      + '&granularity=30'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + '&limit=1'
      + '&after=0ac8dd00086a26594ab800acc5f79c21450ee147'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 1, 'expecting 1 entry for april 2024');
    const expected = [
      {
        content_requests: 18620,
        cr_apicalls: 520,
        cr_margin_of_error: 2765,
        cr_pageviews: 18100,
        error404_requests: 0,
        hostname: 'dxc.com',
        html_requests: 19900,
        id: 'c6dfddbcc0addf585931499895b1f0729d50f390',
        json_requests: 2900,
        month: 4,
        time: '2024-04-01T00:00:00+00:00',
        top_host: 'rum.hlx.page',
        year: 2024,
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);
});
