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
const {
  loadQuery, getHeaderParams, cleanHeaderParams,
  cleanQuery, authFastly, replaceTableNames,
  resolveParameterDiff, cleanRequestParams,
  csvify,
} = require('../src/util.js');
const env = require('../src/env.js');

describe('testing util functions', () => {
  const service = '6E6ge7REhiWetPCqy9jht2';

  it('loadQuery loads a query', async () => {
    const result = await loadQuery('next-resource');
    assert.ok(result.match(/select/i));
  });

  it('loadQuery throws with bad query file', async () => {
    const EXPECTED = new Error('Failed to load .sql file');
    const handle = () => loadQuery('Does not Exist');
    assert.rejects(handle, EXPECTED);
    try {
      await loadQuery('Does not Exist');
    } catch (e) {
      assert.equal(e.statusCode, 404);
    }
  });

  it('query parameters are processed', () => {
    const fakeQuery = '--- helix-param: helix\n--- helix-param2: helix2\n--- helix-param3: helix3\n# this query is intentionally broken.';
    const EXPECTED = { 'helix-param': 'helix', 'helix-param2': 'helix2', 'helix-param3': 'helix3' };
    const ACTUAL = getHeaderParams(fakeQuery);
    assert.deepEqual(EXPECTED, ACTUAL);
  });

  it('query parameters are cleaned from query', () => {
    const fakeQuery = `--- helix-param: helix
--- helix-param2: helix2
--- helix-param3: helix3
#This is A random Comment
SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
    FROM ^tablename
    WHERE 
      resp_http_Content_Type LIKE "text/html%" AND
      status_code LIKE "404"
    GROUP BY
      req_url, resp_http_Content_Type, status_code 
    ORDER BY visits DESC
    LIMIT @limit`;

    const EXPECTED = `SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
    FROM ^tablename
    WHERE 
      resp_http_Content_Type LIKE "text/html%" AND
      status_code LIKE "404"
    GROUP BY
      req_url, resp_http_Content_Type, status_code 
    ORDER BY visits DESC
    LIMIT @limit`;
    const ACTUAL = cleanQuery(fakeQuery);
    assert.equal(EXPECTED, ACTUAL);
  });

  it('authFastly correctly authenticates', async () => {
    const ret = await authFastly(env.token, service);
    assert.equal(true, ret);
  });

  it('authFastly rejects with bad token', async () => {
    const handle = async () => {
      await authFastly('bad token', service);
    };
    assert.rejects(handle);
  });

  it('authFastly rejects with bad serviceid', async () => {
    const handle = async () => {
      await authFastly(env.token, 'bad service');
    };
    assert.rejects(handle);
  });

  it('replaceTableName works', async () => {
    const result = await replaceTableNames('foo ^bar baz', { bar: () => 'bar' });

    assert.equal(result, 'foo bar baz');
  });

  it('replaceTableName works with promises', async () => {
    const result = await replaceTableNames('foo ^bar baz', { bar: () => Promise.resolve('bar') });

    assert.equal(result, 'foo bar baz');
  });

  it('replaceTableName works when not needed', async () => {
    const result = await replaceTableNames('foo bar baz', { bar: () => 'bar' });

    assert.equal(result, 'foo bar baz');
  });

  it('replaceTableName does not repeat', async () => {
    let i = 0;
    const result = await replaceTableNames('foo ^bar ^bar ^bar ^bar ^bar ^bar baz', {
      bar: () => {
        i += 1;
        return 'bar';
      },
    });

    assert.equal(result, 'foo bar bar bar bar bar bar baz');
    assert.equal(i, 1);
  });

  it('resolveParameterDiff fills in empty params with defaults', () => {
    const query = `--- something1: Likes
--- something2: CMS
--- tablename: fakeTable
--- rising: true
--- falling: false
SELECT @something1, @something2 WHERE @tablename`;
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
      something2: 'CMS',
      rising: true,
      falling: false,
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('resolveParameterDiff works if some defaults missing', () => {
    const query = '--- something1: Likes\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('resolveParameterDiff works if all defaults missing', () => {
    const query = 'SELECT @something1, @something2 WHERE @tablename';
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams removes query parameters', () => {
    const query = '--- something1: Likes\n--- something2: CMS\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams, true);
    const EXPECTED = {};

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams removes everything except Headers', () => {
    const query = '--- Cache-Control: max-age=300\n--- something1: Likes\n--- something2: CMS\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams, true);
    const EXPECTED = { 'Cache-Control': 'max-age=300' };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams has no headers or default parameters does not fail', () => {
    const query = 'SELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams);
    const EXPECTED = {};

    assert.deepEqual(ACTUAL, EXPECTED);
  });

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

  it('csvify generates csv', () => {
    const result = csvify([
      { string: 'string', bool: true, num: 1.0 },
      { string: 'str,ong', bool: false, num: -0.1 },
      { string: 'str"ong', bool: false, num: -0.1 },
    ]);
    const expected = `string,bool,num
"string",TRUE,1
"str,ong",FALSE,-0.1
"str""ong",FALSE,-0.1`;
    assert.equal(result, expected);
  });

  it('csvify generates empty csv from empty data', () => {
    const result = csvify([]);
    const expected = '';
    assert.equal(result, expected);
  });
});
