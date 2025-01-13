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

async function retryFetch(url, options, maxRetries = 3, initialDelay = 1000) {
  const attempts = Array.from({ length: maxRetries }, (_, i) => i + 1);
  const MAX_DELAY = 60000; // Cap the maximum delay at 60 seconds

  for (const attempt of attempts) {
    try {
      // eslint-disable-next-line no-await-in-loop
      const response = await fetch(url, options);
      if (response.status !== 503) {
        return response;
      }
      const backoffDelay = Math.min(initialDelay * (2 ** (attempt - 1)), MAX_DELAY);
      console.log(`Attempt ${attempt}: Got 503, retrying in ${backoffDelay}ms...`);
      // eslint-disable-next-line no-await-in-loop
      await setTimeout(backoffDelay);
    } catch (error) {
      if (attempt === maxRetries) throw error;
      const backoffDelay = Math.min(initialDelay * (2 ** (attempt - 1)), MAX_DELAY);
      console.log(`Attempt ${attempt}: Failed with ${error.message}, retrying in ${backoffDelay}ms...`);
      // eslint-disable-next-line no-await-in-loop
      await setTimeout(backoffDelay);
    }
  }
  throw new Error(`Failed after ${maxRetries} attempts`);
}

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

    it('Service reports status', async () => {
      const path = `${target.urlPath()}/_status_check/healthcheck.json`;
      console.log(`testing ${target.host()}${path}`);
      const response = await retryFetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      });
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
    }).timeout(30000);

    it('RUM Dashboard', async () => {
      const path = `${target.urlPath()}/rum-dashboard?url=www.adobe.com`;
      console.log(`testing ${target.host()}${path}`);
      const response = await retryFetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      }, 5, 1000); // Increase max retries to 5 for this endpoint
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
      const body = await response.json();
      assert.equal(body.meta.data.length, 49);
    }).timeout(120000); // Double the timeout

    it('Daily Pageviews', async () => {
      const path = `${target.urlPath()}/rum-pageviews?url=www.theplayers.com&offset=1`;
      console.log(`testing ${target.host()}${path}`);
      const response = await retryFetch(`${target.host()}${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        },
      }, 5, 1000); // Increase max retries to 5 for this endpoint
      assert.equal(response.status, 200, await response.text());
      assert.equal(response.headers.get('Content-Type'), 'application/json');
    }).timeout(120000); // Double the timeout
  }).timeout(180000); // Increase suite timeout
});
