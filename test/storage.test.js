/*
 * Copyright 2021 Adobe. All rights reserved.
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
import fs from 'fs/promises';
import path from 'path';
import { promisify } from 'util';
import zlib from 'zlib';
import * as sinon from 'sinon';
import { S3Client } from '@aws-sdk/client-s3';
import { Response } from '@adobe/fetch';
import { Bucket, HelixStorage } from '../src/storage.js';

/**
 * @typedef {import('../src/storage.js').Bucket} Bucket
 * @typedef {import('@aws-sdk/client-s3').$Command} AWSCommand
 * @typedef {import('@aws-sdk/client-s3').ServiceOutputTypes} AWSServiceOutputTypes
 * @typedef {import('@aws-sdk/client-s3').S3Client} AWSS3Client
 */

const gzip = promisify(zlib.gzip);

const AWS_REGION = 'fake';
const AWS_ACCESS_KEY_ID = 'fake';
const AWS_SECRET_ACCESS_KEY = 'fake';

const CLOUDFLARE_ACCOUNT_ID = 'fake';
const CLOUDFLARE_R2_ACCESS_KEY_ID = 'fake';
const CLOUDFLARE_R2_SECRET_ACCESS_KEY = 'fake';

/**
 * @param {Bucket} bucket
 * @param {AWSServiceOutputTypes|{s3:AWSServiceOutputTypes[]; r2:AWSServiceOutputTypes[]}} resp
 * @param {(command: AWSCommand, i: number, type: 'r2'|'s3') => void|Promise<void>} cb
 * @param {boolean} [onlyS3]
 * @returns {Promise<void>}
 */
async function stubBucketSend(bucket, resp, cb = () => {}, onlyS3 = false) {
  let res1;
  let res2;
  const p1 = new Promise((res) => {
    res1 = res;
  });
  const p2 = onlyS3 ? Promise.resolve() : new Promise((res) => {
    res2 = res;
  });

  /** @type {[AWSS3Client, AWSS3Client]} */
  // eslint-disable-next-line no-underscore-dangle
  const [s3Client, r2Client] = bucket._clients;

  let { s3Resps, r2Resps } = resp.r2 && resp.s3
    ? { s3Resps: resp.s3, r2Resps: resp.r2 }
    : { s3Resps: resp, r2Resps: resp };
  if (!Array.isArray(s3Resps)) {
    s3Resps = [s3Resps];
  }
  if (!Array.isArray(r2Resps)) {
    r2Resps = [r2Resps];
  }

  const s1 = sinon.stub(s3Client, 'send');
  let i = 0;
  s1.callsFake(async (command) => {
    await cb(command, i, 's3');
    const s3Resp = s3Resps[i];
    i += 1;
    if (i >= s3Resps.length) {
      res1();
    }
    if (s3Resp instanceof Error) {
      throw s3Resp;
    }
    return s3Resp;
  });

  if (!onlyS3) {
    const s2 = sinon.stub(r2Client, 'send');
    let j = 0;
    s2.callsFake(async (command) => {
      await cb(command, j, 'r2');
      const r2Resp = r2Resps[i];
      j += 1;
      if (j >= r2Resps.length) {
        res2();
      }
      if (r2Resp instanceof Error) {
        throw r2Resp;
      }
      return r2Resp;
    });
  }

  await Promise.all([p1, p2]);
}

describe('Storage test', () => {
  // let nock;
  /** @type {HelixStorage} */
  let storage;

  beforeEach(() => {
    storage = new HelixStorage({
      region: AWS_REGION,
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
      r2AccountId: CLOUDFLARE_ACCOUNT_ID,
      r2AccessKeyId: CLOUDFLARE_R2_ACCESS_KEY_ID,
      r2SecretAccessKey: CLOUDFLARE_R2_SECRET_ACCESS_KEY,
    });
  });

  afterEach(() => {
    storage.close();
  });

  it('bucket() needs bucket', () => {
    assert.throws(() => storage.bucket(), Error('bucketId is required.'));
  });

  it('contentBus() fails on closed storage', () => {
    storage.close();
    assert.throws(() => storage.contentBus(), Error('storage already closed.'));
  });

  it('mediaBus() returns Bucket', () => {
    assert.ok(storage.mediaBus() instanceof Bucket);
  });

  it('configBus() returns Bucket', () => {
    assert.ok(storage.configBus() instanceof Bucket);
  });

  it('s3() returns S3Client', () => {
    assert.ok(storage.s3() instanceof S3Client);
  });

  it('can put object', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, {}, async (command) => {
      assert.equal(command.input.Key, 'foo');
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.equal(command.input.ContentType, 'text/plain');
      assert.equal(command.input.ContentEncoding, 'gzip');
      assert.deepEqual(command.input.Body, await gzip(Buffer.from('hello, world.', 'utf-8')));
      assert.deepEqual(command.input.Metadata, { myid: '1234' });
    });

    await bus.put('/foo', 'hello, world.', 'text/plain', {
      myid: '1234',
    });

    await p;
  });

  it('can put object uncompressed', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, {}, async (command) => {
      assert.equal(command.input.Key, 'foo');
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.equal(command.input.ContentType, 'text/plain');
      assert.equal(command.input.Body, 'hello, world.');
      assert.deepEqual(command.input.Metadata, { myid: '1234' });
    });

    await bus.put('/foo', 'hello, world.', 'text/plain', {
      myid: '1234',
    }, false);

    await p;
  });

  it('can remove object', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, {}, async (command) => {
      assert.equal(command.input.Key, 'foo');
      assert.equal(command.input.Bucket, 'helix-code-bus');
    });

    await bus.remove('/foo');

    await p;
  });

  it('remove non-existing object fails', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, Error('does not exist'));
    await assert.rejects(async () => bus.remove('/does-not-exist'));
    await p;
  });

  it('can remove objects', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, { Deleted: ['foo', 'bar'] }, async (command) => {
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.deepEqual(command.input.Delete, { Objects: [{ Key: 'foo' }, { Key: 'bar' }] });
    });
    await bus.remove(['/foo', '/bar']);

    await p;
  });

  it('can copy objects', async () => {
    const listReply = JSON.parse(
      await fs.readFile(path.resolve(__testdir, 'fixtures', 'storage', 'list-reply-copy.json'), 'utf-8'),
    );
    const bus = storage.codeBus();
    const puts = { s3: [], r2: [] };

    const p = stubBucketSend(
      bus,
      { s3: [...listReply, {}], r2: [{}] },
      async (command, i, backend) => {
        if (command.constructor.name === 'CopyObjectCommand') {
          puts[backend].push(command.input.Key);
        }
      },
    );
    await bus.copyDeep('/owner/repo/ref/', '/bar/');
    await p;

    puts.s3.sort();
    puts.r2.sort();
    const expectedPuts = [
      'bar/.circleci/config.yml',
      'bar/.gitignore',
      'bar/.vscode/launch.json',
      'bar/.vscode/settings.json',
      'bar/README.md',
      'bar/helix_logo.png',
      'bar/htdocs/favicon.ico',
      'bar/htdocs/style.css',
      'bar/index.md',
      'bar/src/html.pre.js',
    ];
    assert.deepEqual(puts.s3, expectedPuts);
    assert.deepEqual(puts.r2, expectedPuts);
  });

  it('can delete objects', async () => {
    const listReply = JSON.parse(
      await fs.readFile(path.resolve(__testdir, 'fixtures', 'storage', 'list-reply.json'), 'utf-8'),
    );
    const bus = storage.codeBus();
    const deletes = { s3: [], r2: [] };

    const p = stubBucketSend(
      bus,
      { s3: [listReply, {}], r2: [{}] },
      async (command, i, backend) => {
        if (command.constructor.name === 'DeleteObjectCommand') {
          deletes[backend].push(command.input.Key);
        }
      },
    );
    await bus.rmdir('/owner/repo/new-branch/');
    await p;

    deletes.s3.sort();
    deletes.r2.sort();
    const expectedDeletes = [
      'owner/repo/ref/.circleci/config.yml',
      'owner/repo/ref/.gitignore',
      'owner/repo/ref/.vscode/launch.json',
      'owner/repo/ref/.vscode/settings.json',
      'owner/repo/ref/README.md',
      'owner/repo/ref/helix_logo.png',
      'owner/repo/ref/htdocs/favicon.ico',
      'owner/repo/ref/htdocs/style.css',
      'owner/repo/ref/index.md',
      'owner/repo/ref/src/html.pre.js',
    ];
    assert.deepEqual(deletes.s3, expectedDeletes);
    assert.deepEqual(deletes.r2, expectedDeletes);
  });

  it('rmdir works for empty dir', async () => {
    const resp = {
      Name: 'helix-code-bus',
      Prefix: 'owner/repo/new-branch/',
      KeyCount: 0,
      MaxKeys: 1000,
      IsTruncated: false,
    };
    const bus = storage.codeBus();

    const p = stubBucketSend(
      bus,
      resp,
      undefined,
      true,
    );

    await bus.rmdir('/owner/repo/new-branch/');
    await p;
  });

  it('can list folders', async () => {
    const listReply = JSON.parse(
      await fs.readFile(path.resolve(__testdir, 'fixtures', 'storage', 'list-folders-reply.json'), 'utf-8'),
    );
    const bus = storage.codeBus();

    const p = stubBucketSend(
      bus,
      listReply,
      undefined,
      true,
    );

    const folders = await bus.listFolders('');
    await p;

    assert.deepStrictEqual(folders, ['owner/', 'other/']);
  });

  it('can return an empty list of folders', async () => {
    const resp = {
      KeyCount: 0,
    };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, resp, undefined, true);
    const folders = await bus.listFolders('foo/');
    await p;

    assert.deepStrictEqual(folders, []);
  });

  it('can get metadata of object', async () => {
    const headResp = {
      Metadata: { foo: true },
    };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, headResp, undefined, true);
    const meta = await bus.metadata('/foo');
    await p;

    assert.deepStrictEqual(meta, { foo: true });
  });

  it('head() 404 returns null', async () => {
    const err = new Error('bad');
    err.$metadata = { httpStatusCode: 404 };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, err, undefined, true);
    const data = await bus.head('foo');
    await p;

    assert.deepStrictEqual(data, null);
  });

  it('get() 404 returns null', async () => {
    const err = new Error('bad');
    err.$metadata = { httpStatusCode: 404 };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, err, undefined, true);
    const data = await bus.get('/foo');
    await p;

    assert.deepStrictEqual(data, null);
  });

  it('get() uncompressed object', async () => {
    const getResp = {
      Body: 'foo',
      ContentType: 'text/plain',
    };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, getResp, undefined, true);
    const data = await bus.get('/foo');
    await p;

    assert.deepStrictEqual(data, Buffer.from('foo', 'utf-8'));
  });

  it('get() gzipped object', async () => {
    const getResp = {
      Body: await gzip(Buffer.from('foo', 'utf-8')),
      ContentType: 'text/plain',
      ContentEncoding: 'gzip',
    };

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, getResp, undefined, true);
    const data = await bus.get('/foo');
    await p;

    assert.deepStrictEqual(data, Buffer.from('foo', 'utf-8'));
  });

  it('can store response', async () => {
    const resp = new Response('foo', {
      headers: {
        'content-type': 'text/plain',
        'last-modified': 0,
        'x-other-header': 'bar',
      },
    });

    const bus = storage.codeBus();
    const p = stubBucketSend(bus, resp, async (command) => {
      assert.equal(command.input.Key, 'foo');
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.equal(command.input.ContentEncoding, 'gzip');
      assert.deepEqual(command.input.Body, await gzip(Buffer.from('foo', 'utf-8')));
      assert.deepEqual(command.input.Metadata, {
        'x-source-last-modified': 0,
        'x-other-header': 'bar',
      });
    });
    await bus.store('/foo', resp);
    await p;
  });

  it('can put metadata', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, {}, async (command) => {
      assert.equal(command.input.Key, 'foo');
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.equal(command.input.CopySource, 'helix-code-bus/foo');
      assert.equal(command.input.MetadataDirective, 'REPLACE');
      assert.deepEqual(command.input.Metadata, { bar: true, baz: 123 });
    });
    await bus.putMeta('/foo', { bar: true, baz: 123 });
    await p;
  });

  it('can copy', async () => {
    const bus = storage.codeBus();
    const p = stubBucketSend(bus, {}, async (command) => {
      assert.equal(command.input.Key, 'bar');
      assert.equal(command.input.Bucket, 'helix-code-bus');
      assert.equal(command.input.CopySource, 'helix-code-bus/foo');
    });
    await bus.copy('/foo', '/bar');
    await p;
  });

  it('copy() 404 rejects', async () => {
    const err = new Error();
    err.Code = 'NoSuchKey';
    const bus = storage.codeBus();

    stubBucketSend(bus, err);
    await assert.rejects(bus.copy('/foo', '/bar'), 'source does not exist');
  });

  it('remove() 404 rejects promise', async () => {
    const err = new Error('bad');
    err.$metadata = { httpStatusCode: 404 };
    const bus = storage.codeBus();

    stubBucketSend(bus, err);
    await assert.rejects(bus.remove(['/foo']));
  });

  it('HelixStorage~fromContext()', async () => {
    const ctx = {
      attributes: {},
      env: {},
    };

    const storage1 = HelixStorage.fromContext(ctx);
    assert.ok(storage1 instanceof HelixStorage);

    const storage2 = HelixStorage.fromContext(ctx);
    assert.equal(storage1, storage2);
  });
});
