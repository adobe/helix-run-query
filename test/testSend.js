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
const assert = require('assert');
const sinon = require('sinon');
const { AssertionError } = require('assert');
const proxyquire = require('proxyquire');
const env = require('../src/env.js');
const { loadQuery, getExtraParameters, queryReplace } = require('../src/util.js');
const { execute } = require('../src/sendquery.js');

describe('bigquery tests', () => {
  const goodQuery = 'select * from requests201905';
  const badQuery = '# this query is intentionally broken.';

  const badExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => badQuery } });
  const goodExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQuery } });

  it('runs a query', async () => {
    const { results } = await goodExec.execute(env.email, env.key, env.projectid, 'list-everything', '0bxMEaYAJV6SoqFlbZ2n1f');
    assert.ok(Array.isArray(results));
  }).timeout(5000);

  it('runs a query with params', async () => {
    const { results } = await goodExec.execute(env.email, env.key, env.projectid, 'list-everything', '0bxMEaYAJV6SoqFlbZ2n1f', {
      limit: 10,
    });
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 10);
  }).timeout(5000);

  it('runs a query with queryParams', async () => {
    const { results } = await execute(env.email, env.key, env.projectid, 'next-resource', '0bxMEaYAJV6SoqFlbZ2n1f', {
      limit: 1,
      tablename: 'requests201905',
    });
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 1);
  }).timeout(1000000);

  it('throws without projectid', async () => {
    try {
      await goodExec.execute(env.email, env.key, undefined, 'list-everything', '0bxMEaYAJV6SoqFlbZ2n1f');
      assert.fail('expected exception not thrown');
    } catch (e) {
      if (e instanceof AssertionError) {
        // bubble up the assertion error
        throw e;
      }
    }
  }).timeout(5000);

  it('throws with bad query', async () => {
    try {
      await badExec.execute(env.email, env.key, env.projectid, 'break-something', '0bxMEaYAJV6SoqFlbZ2n1f');
      assert.fail('expected exception not thrown');
    } catch (e) {
      sinon.restore();
      if (e instanceof AssertionError) {
        // bubble up the assertion error
        throw e;
      }
    }
  }).timeout(5000);
});

describe('sql loading and processing', () => {
  it('loadQuery loads a query', () => {
    const result = loadQuery('next-resource');
    assert.ok(result.match(/select/i));
  });

  it('query parameters are processed', () => {
    const fakeQuery = '--- helix-param: helix\n--- helix-param2: helix2\n--- helix-param3: helix3\n# this query is intentionally broken.';
    const EXPECTED = { 'helix-param': 'helix', 'helix-param2': 'helix2', 'helix-param3': 'helix3' };
    const ACTUAL = getExtraParameters(fakeQuery);
    assert.deepEqual(EXPECTED, ACTUAL);
  });

  it('query substitution works', () => {
    const query = 'SELECT ^something1, ^something2 WHERE ^tablename';
    const EXPECTED = 'SELECT `Loves`, `Lucy` WHERE `Helix`';

    const params = {
      tablename: 'Helix',
      something1: 'Loves',
      something2: 'Lucy',
    };

    const ACTUAL = queryReplace(query, params);

    assert.equal(ACTUAL, EXPECTED);
  });

  it('prevents sql injection by canceling double,single,or template strings', () => {
    const query = 'SELECT ^something1, ^something2 WHERE ^tablename';
    const EXPECTED = 'SELECT `Loves`, `CMS` WHERE `Helix`';

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
      something2: '"CMS',
      something3: 'foobar',
    };

    const ACTUAL = queryReplace(query, params);

    assert.equal(ACTUAL, EXPECTED);
  });
});
