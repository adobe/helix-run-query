/*
 * Copyright 2018 Adobe. All rights reserved.
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
import { setTimeout } from 'node:timers/promises';
import { fetch } from '@adobe/fetch';
import { createTargets } from './post-deploy-utils.js';

createTargets().forEach((target) => {
  describe(`Post-Deploy Tests (${target.title()}) ${target.host()}${target.urlPath()}`, () => {
    before(async function beforeAll() {
      if (!target.enabled()) {
        this.skip();
      } else {
        console.log('wait for 2 seconds for function become ready.... (really?)');
        this.timeout(3000);
        await setTimeout(2000);
      }
    });

    it('RUM Dashboard', async () => {
      const path = `${target.urlPath()}/rum-dashboard`;
      // eslint-disable-next-line no-console
      console.log(`testing ${target.host()}${path}`);
      const response = await fetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      });
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
      const body = await response.json();
      assert.equal(body.meta.data.length, 49);
    }).timeout(60000);

    it('Daily Pageviews', async () => {
      const path = `${target.urlPath()}/rum-pageviews?url=www.theplayers.com&offset=1`;
      // eslint-disable-next-line no-console
      console.log(`testing ${target.host()}${path}`);
      const response = await fetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      });
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
    }).timeout(60000);

    it('Service reports status', async () => {
      const path = `${target.urlPath()}/_status_check/healthcheck.json`;
      // eslint-disable-next-line no-console
      console.log(`testing ${target.host()}${path}`);
      const response = await fetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      });
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
    }).timeout(10000);
  }).timeout(60000);
});
