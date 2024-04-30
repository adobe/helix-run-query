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
import assert from 'assert';
import {
  chartify,
  cleanHeaderParams,
  cleanRequestParams, csvify,
  getHeaderParams,
  getTrailingParams,
  loadQuery, resolveParameterDiff,
  sshonify,
  validParamCheck, extractQueryPath,
} from '../src/util.js';

describe('testing util functions', () => {
  it('extractQueryPath parses a run-query path to find the folder/file for a query', async () => {
    assert.equal(extractQueryPath('/helix-services/run-query/file'), 'file');
    assert.equal(extractQueryPath('/helix-services/run-query@v1/file'), 'file');
    assert.equal(extractQueryPath('/helix-services/run-query@v2/folder/file'), 'folder/file');
    assert.equal(extractQueryPath('/helix-services/run-query@ci123/file'), 'file');
    assert.equal(extractQueryPath('/helix-services/run-query@ci456/folder/file'), 'folder/file');
    assert.equal(extractQueryPath('/helix-services/run-query/ci789/file'), 'file');
    assert.equal(extractQueryPath('/helix-services/run-query/ci123/folder/file'), 'folder/file');
    assert.equal(extractQueryPath('/helix-services/run-query@v3/folder/folder/file'), 'folder/folder/file');
    assert.equal(extractQueryPath('/helix-services/run-query/3.3.0/folder/folder/file'), 'folder/folder/file');
  });

  it('loadQuery loads a query', async () => {
    const result = await loadQuery('rum-dashboard');
    assert.ok(result.match(/select/i));
  });

  it('loadQuery works with trailing parameters', async () => {
    const result = await loadQuery('rum-dashboard');
    assert.equal(Object.keys(getTrailingParams(result)).length, 37);
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

  it('validParamCheck returns false if there is array in param', () => {
    const badParams = {
      one: 'okay',
      two: 'okay',
      three: [1, 2, 3, 4, 5],
      four: 'okay',
    };

    const EXPECTED_STATUS = '400';
    const EXPECTED_MESSAGE = 'Duplicate URL parameters found';

    try {
      validParamCheck(badParams);
    } catch (e) {
      assert.deepEqual(EXPECTED_STATUS, e.statusCode);
      assert.deepEqual(EXPECTED_MESSAGE, e.message);
    }
  });

  it('resolveParameterDiff will throw error with 400 if array detected', () => {
    const badParams = {
      one: 'okay',
      two: 'okay',
      three: [1, 2, 3, 4, 5],
      four: 'okay',
    };

    const EXPECTED_STATUS = 400;
    const EXPECTED_MESSAGE = 'Duplicate URL parameters found';

    try {
      resolveParameterDiff(badParams, {});
    } catch (e) {
      assert.deepEqual(EXPECTED_STATUS, e.statusCode);
      assert.deepEqual(EXPECTED_MESSAGE, e.message);
    }
  });
});

describe('Test SSHONify', () => {
  it('SSHONify generates SSHON', () => {
    const input = {
      results: [{
        checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
      }],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30, interval: 1, offset: '0', url: 'blog.adobe.com', checkpoint: 'viewblock', source: '-',
      },
      responseDetails: {
        checkpoint: 'name of the checkpoint, i.e. the event in the page load or interaction sequence that was observed.',
        source: 'CSS id or class name of the element that triggered the checkpoint.',
        ids: 'number of unique RUM ids that triggered the checkpoint.',
        pages: 'number of unique pages that triggered the checkpoint.',
        topurl: 'most frequently observed URL that triggered the checkpoint.',
        views: 'interpolated number of pageviews that triggered the checkpoint.',
        actions: 'number of times the checkpoint was triggered. This may be greater than the number of unique ids if the same id triggered the checkpoint multiple times.',
        actions_per_view: 'average number of times the checkpoint was triggered per pageview.',
      },
      truncated: false,
    };
    const sshon = sshonify(
      input.results,
      input.description,
      input.requestParams,
      input.responseDetails,
      {},
      input.truncated,
    );
    assert.deepStrictEqual(
      JSON.parse(sshon),
      {
        ':names': [
          'results',
          'meta',
        ],
        ':type': 'multi-sheet',
        ':version': 3,
        meta: {
          data: [
            {
              name: 'description',
              type: 'query description',
              value: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
            },
            {
              name: 'limit',
              type: 'request parameter',
              value: 30,
            },
            {
              name: 'interval',
              type: 'request parameter',
              value: 1,
            },
            {
              name: 'offset',
              type: 'request parameter',
              value: '0',
            },
            {
              name: 'url',
              type: 'request parameter',
              value: 'blog.adobe.com',
            },
            {
              name: 'checkpoint',
              type: 'request parameter',
              value: 'viewblock',
            },
            {
              name: 'source',
              type: 'request parameter',
              value: '-',
            },
            {
              name: 'checkpoint',
              type: 'response detail',
              value: 'name of the checkpoint, i.e. the event in the page load or interaction sequence that was observed.',
            },
            {
              name: 'source',
              type: 'response detail',
              value: 'CSS id or class name of the element that triggered the checkpoint.',
            },
            {
              name: 'ids',
              type: 'response detail',
              value: 'number of unique RUM ids that triggered the checkpoint.',
            },
            {
              name: 'pages',
              type: 'response detail',
              value: 'number of unique pages that triggered the checkpoint.',
            },
            {
              name: 'topurl',
              type: 'response detail',
              value: 'most frequently observed URL that triggered the checkpoint.',
            },
            {
              name: 'views',
              type: 'response detail',
              value: 'interpolated number of pageviews that triggered the checkpoint.',
            },
            {
              name: 'actions',
              type: 'response detail',
              value: 'number of times the checkpoint was triggered. This may be greater than the number of unique ids if the same id triggered the checkpoint multiple times.',
            },
            {
              name: 'actions_per_view',
              type: 'response detail',
              value: 'average number of times the checkpoint was triggered per pageview.',
            },
          ],
          columns: [
            'name',
            'value',
            'type',
          ],
          limit: 7,
          offset: 0,
          total: 7,
        },
        results: {
          limit: 30,
          offset: 0,
          total: 30,
          columns: [
            'checkpoint',
            'source',
            'ids',
            'pages',
            'topurl',
            'views',
            'actions',
            'actions_per_view',

          ],
          data: [{
            checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
          }],
        },
      },

    );
  });

  it('SSHONify generates SSHON from empty results', () => {
    const input = {
      results: [],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30, interval: 1, offset: '0', url: 'blog.adobe.com', checkpoint: 'viewblock', source: '-',
      },
      truncated: false,
    };
    const sshon = sshonify(
      input.results,
      input.description,
      input.requestParams,
      {},
      {},
      input.truncated,
    );
    assert.deepStrictEqual(
      JSON.parse(sshon),
      {
        ':names': [
          'results',
          'meta',
        ],
        ':type': 'multi-sheet',
        ':version': 3,
        meta: {
          data: [
            {
              name: 'description',
              type: 'query description',
              value: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
            },
            {
              name: 'limit',
              type: 'request parameter',
              value: 30,
            },
            {
              name: 'interval',
              type: 'request parameter',
              value: 1,
            },
            {
              name: 'offset',
              type: 'request parameter',
              value: '0',
            },
            {
              name: 'url',
              type: 'request parameter',
              value: 'blog.adobe.com',
            },
            {
              name: 'checkpoint',
              type: 'request parameter',
              value: 'viewblock',
            },
            {
              name: 'source',
              type: 'request parameter',
              value: '-',
            },
          ],
          columns: [
            'name',
            'value',
            'type',
          ],
          limit: 7,
          offset: 0,
          total: 7,
        },
        results: {
          limit: 30,
          offset: 0,
          total: 0,
          columns: [],
          data: [],
        },
      },

    );
  });
});

describe('Test chartify', () => {
  it('chartify generates chart', () => {
    const chartconfig = {
      type: 'horizontalBar',
      data: {
        labels: '@source',
        datasets: [
          {
            label: 'Views',
            backgroundColor: 'rgba(255, 99, 132, 0.5)',
            borderColor: 'rgb(255, 99, 132)',
            borderWidth: 1,
            data: '@views',
          },
        ],
      },
      options: {
        elements: {
          rectangle: {
            borderWidth: 2,
          },
        },
        responsive: true,
        legend: {
          position: 'right',
        },
        title: {
          display: true,
          text: 'Most popular Blocks by Views',
        },
      },
    };
    const input = {
      results: [{
        checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
      }],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30,
        interval: 1,
        offset: '0',
        url: 'blog.adobe.com',
        checkpoint: 'viewblock',
        source: '-',
        chart: JSON.stringify(chartconfig),
      },
      responseDetails: {
        checkpoint: 'name of the checkpoint, i.e. the event in the page load or interaction sequence that was observed.',
        source: 'CSS id or class name of the element that triggered the checkpoint.',
        ids: 'number of unique RUM ids that triggered the checkpoint.',
        pages: 'number of unique pages that triggered the checkpoint.',
        topurl: 'most frequently observed URL that triggered the checkpoint.',
        views: 'interpolated number of pageviews that triggered the checkpoint.',
        actions: 'number of times the checkpoint was triggered. This may be greater than the number of unique ids if the same id triggered the checkpoint multiple times.',
        actions_per_view: 'average number of times the checkpoint was triggered per pageview.',
      },
      truncated: false,
    };
    const chart = chartify(
      input.results,
      input.description,
      input.requestParams,
    );
    assert(chart);
    assert.equal(chart, '{"type":"horizontalBar","data":{"labels":[".article-header",".images",".banner",".tags",".recommended-articles",".embed",".animation",".featured-article",".video",".article-feed",".tag-header",".marquee",".table-of-contents",".error-attr",".error-text",".pull-quote",".author-header",".toolkit",".newsletter-modal",".image","#tabs-0",".infographic",".columns",".infograph",".art-culos-recomendados",".social-links",".carousel",".namecard",".image-50",".full-width-banner"],"datasets":[{"label":"Views","backgroundColor":"rgba(255, 99, 132, 0.5)","borderColor":"rgb(255, 99, 132)","borderWidth":1,"data":["32580","21000","13500","13110","11170","7690","4730","4440","4200","3590","2220","1790","1530","650","650","470","380","270","260","200","150","140","140","100","50","50","30","30","20","20"]}]},"options":{"elements":{"rectangle":{"borderWidth":2}},"responsive":true,"legend":{"position":"right"},"title":{"display":true,"text":"Most popular Blocks by Views"}}}');
  });

  it('chartify generates scatterplot', () => {
    const chartconfig = {
      type: 'scatter',
      data: {
        labels: '@source',
        datasets: [
          {
            label: 'Views',
            backgroundColor: 'rgba(255, 99, 132, 0.5)',
            borderColor: 'rgb(255, 99, 132)',
            borderWidth: 1,
            data: '@ids,@pages',
          },
        ],
      },
      options: {
        elements: {
          rectangle: {
            borderWidth: 2,
          },
        },
        responsive: true,
        legend: {
          position: 'right',
        },
        title: {
          display: true,
          text: 'Most popular Blocks by Views',
        },
      },
    };
    const input = {
      results: [{
        checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
      }],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30,
        interval: 1,
        offset: '0',
        url: 'blog.adobe.com',
        checkpoint: 'viewblock',
        source: '-',
        chart: JSON.stringify(chartconfig),
      },
      responseDetails: {
        checkpoint: 'name of the checkpoint, i.e. the event in the page load or interaction sequence that was observed.',
        source: 'CSS id or class name of the element that triggered the checkpoint.',
        ids: 'number of unique RUM ids that triggered the checkpoint.',
        pages: 'number of unique pages that triggered the checkpoint.',
        topurl: 'most frequently observed URL that triggered the checkpoint.',
        views: 'interpolated number of pageviews that triggered the checkpoint.',
        actions: 'number of times the checkpoint was triggered. This may be greater than the number of unique ids if the same id triggered the checkpoint multiple times.',
        actions_per_view: 'average number of times the checkpoint was triggered per pageview.',
      },
      truncated: false,
    };
    const chart = chartify(
      input.results,
      input.description,
      input.requestParams,
    );
    assert(chart);
    assert.equal(chart, '{"type":"scatter","data":{"labels":[".article-header",".images",".banner",".tags",".recommended-articles",".embed",".animation",".featured-article",".video",".article-feed",".tag-header",".marquee",".table-of-contents",".error-attr",".error-text",".pull-quote",".author-header",".toolkit",".newsletter-modal",".image","#tabs-0",".infographic",".columns",".infograph",".art-culos-recomendados",".social-links",".carousel",".namecard",".image-50",".full-width-banner"],"datasets":[{"label":"Views","backgroundColor":"rgba(255, 99, 132, 0.5)","borderColor":"rgb(255, 99, 132)","borderWidth":1,"data":[{"x":3258,"y":1031},{"x":2100,"y":651},{"x":1350,"y":294},{"x":1311,"y":383},{"x":1117,"y":271},{"x":769,"y":226},{"x":473,"y":55},{"x":444,"y":36},{"x":420,"y":24},{"x":359,"y":105},{"x":222,"y":63},{"x":179,"y":55},{"x":153,"y":66},{"x":65,"y":44},{"x":65,"y":44},{"x":47,"y":27},{"x":38,"y":28},{"x":27,"y":5},{"x":26,"y":16},{"x":20,"y":7},{"x":15,"y":1},{"x":14,"y":11},{"x":14,"y":2},{"x":10,"y":4},{"x":5,"y":1},{"x":5,"y":5},{"x":3,"y":1},{"x":3,"y":1},{"x":2,"y":1},{"x":2,"y":1}]}]},"options":{"elements":{"rectangle":{"borderWidth":2}},"responsive":true,"legend":{"position":"right"},"title":{"display":true,"text":"Most popular Blocks by Views"}}}');
  });

  it('chartify generates chart from string config', () => {
    const chartconfig = `{
      type: 'horizontalBar',
      data: {
        labels: @source,
        datasets: [
          {
            label: 'Views',
            backgroundColor: getGradientFillHelper('vertical', ['#eb3639', '#a336eb', '#36a2eb']),
            borderWidth: 1,
            data: @pages,
          },
        ],
      },
      options: {
        elements: {
          rectangle: {
            borderWidth: 2,
          },
        },
        responsive: true,
        legend: {
          position: 'right',
        },
        title: {
          display: true,
          text: 'Most popular Blocks by number of pages on which they were viewed',
        },
      },
    }`;
    const input = {
      results: [{
        checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
      }],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30,
        interval: 1,
        offset: '0',
        url: 'blog.adobe.com',
        checkpoint: 'viewblock',
        source: '-',
        chart: chartconfig,
      },
      responseDetails: {
        checkpoint: 'name of the checkpoint, i.e. the event in the page load or interaction sequence that was observed.',
        source: 'CSS id or class name of the element that triggered the checkpoint.',
        ids: 'number of unique RUM ids that triggered the checkpoint.',
        pages: 'number of unique pages that triggered the checkpoint.',
        topurl: 'most frequently observed URL that triggered the checkpoint.',
        views: 'interpolated number of pageviews that triggered the checkpoint.',
        actions: 'number of times the checkpoint was triggered. This may be greater than the number of unique ids if the same id triggered the checkpoint multiple times.',
        actions_per_view: 'average number of times the checkpoint was triggered per pageview.',
      },
      truncated: false,
    };
    const chart = chartify(
      input.results,
      input.description,
      input.requestParams,
    );
    assert(chart);
    const expected = `{
      type: 'horizontalBar',
      data: {
        labels: [".article-header",".images",".banner",".tags",".recommended-articles",".embed",".animation",".featured-article",".video",".article-feed",".tag-header",".marquee",".table-of-contents",".error-attr",".error-text",".pull-quote",".author-header",".toolkit",".newsletter-modal",".image","#tabs-0",".infographic",".columns",".infograph",".art-culos-recomendados",".social-links",".carousel",".namecard",".image-50",".full-width-banner"],
        datasets: [
          {
            label: 'Views',
            backgroundColor: getGradientFillHelper('vertical', ['#eb3639', '#a336eb', '#36a2eb']),
            borderWidth: 1,
            data: [1031,651,294,383,271,226,55,36,24,105,63,55,66,44,44,27,28,5,16,7,1,11,2,4,1,5,1,1,1,1],
          },
        ],
      },
      options: {
        elements: {
          rectangle: {
            borderWidth: 2,
          },
        },
        responsive: true,
        legend: {
          position: 'right',
        },
        title: {
          display: true,
          text: 'Most popular Blocks by number of pages on which they were viewed',
        },
      },
    }`;
    assert.equal(chart, expected);
  });
});
