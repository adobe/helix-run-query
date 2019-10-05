## [1.2.2](https://github.com/adobe/helix-run-query/compare/v1.2.1...v1.2.2) (2019-10-05)


### Bug Fixes

* **deps:** update any ([#15](https://github.com/adobe/helix-run-query/issues/15)) ([9cae13b](https://github.com/adobe/helix-run-query/commit/9cae13b))

## [1.2.1](https://github.com/adobe/helix-run-query/compare/v1.2.0...v1.2.1) (2019-09-30)


### Bug Fixes

* **monitoring:** update automation command and add config ([c6793a6](https://github.com/adobe/helix-run-query/commit/c6793a6))
* **package:** update @adobe/helix-status to version 5.0.1 ([71643f3](https://github.com/adobe/helix-run-query/commit/71643f3)), closes [#10](https://github.com/adobe/helix-run-query/issues/10)

# [1.2.0](https://github.com/adobe/helix-run-query/compare/v1.1.3...v1.2.0) (2019-09-27)


### Bug Fixes

* **index:** report missing queries as 404 ([cb5d3af](https://github.com/adobe/helix-run-query/commit/cb5d3af)), closes [#1](https://github.com/adobe/helix-run-query/issues/1)
* **lint:** fix lint issues ([da4a3e2](https://github.com/adobe/helix-run-query/commit/da4a3e2))


### Features

* **health:** adds health checking for required services ([0d559a8](https://github.com/adobe/helix-run-query/commit/0d559a8))
* **monitoring:** adds epsagon for service monitoring ([384f271](https://github.com/adobe/helix-run-query/commit/384f271)), closes [#6](https://github.com/adobe/helix-run-query/issues/6)

## [1.1.3](https://github.com/adobe/helix-run-query/compare/v1.1.2...v1.1.3) (2019-09-27)


### Bug Fixes

* **index:** guard against too large response sizes ([2797c94](https://github.com/adobe/helix-run-query/commit/2797c94)), closes [#2](https://github.com/adobe/helix-run-query/issues/2)

## [1.1.2](https://github.com/adobe/helix-run-query/compare/v1.1.1...v1.1.2) (2019-09-27)


### Bug Fixes

* **query:** coerce string number to int for limit ([a083471](https://github.com/adobe/helix-run-query/commit/a083471)), closes [#3](https://github.com/adobe/helix-run-query/issues/3)

## [1.1.1](https://github.com/adobe/helix-run-query/compare/v1.1.0...v1.1.1) (2019-09-27)


### Bug Fixes

* **build:** include sql files in deployment package ([8a25094](https://github.com/adobe/helix-run-query/commit/8a25094))

# [1.1.0](https://github.com/adobe/helix-run-query/compare/v1.0.0...v1.1.0) (2019-09-27)


### Features

* **index:** include error message in response ([51ca09b](https://github.com/adobe/helix-run-query/commit/51ca09b))

# 1.0.0 (2019-09-27)


### Bug Fixes

* **build:** add missing dependency ([aa163d7](https://github.com/adobe/helix-run-query/commit/aa163d7))
* **build:** add wsk property for release tracking ([9e36a10](https://github.com/adobe/helix-run-query/commit/9e36a10))
* **build:** increase version number to get a release ([f04ab95](https://github.com/adobe/helix-run-query/commit/f04ab95))
* **package:** fix npm test script in order to provide information about failing tests ([80e7fe2](https://github.com/adobe/helix-run-query/commit/80e7fe2)), closes [#13](https://github.com/adobe/helix-run-query/issues/13)
* **package:** update @adobe/helix-status to version 4.2.1 ([f7ab47a](https://github.com/adobe/helix-run-query/commit/f7ab47a))
* **package.json:** remove redundant istanbul dependency and use latest nyc ([b29f97d](https://github.com/adobe/helix-run-query/commit/b29f97d)), closes [#4](https://github.com/adobe/helix-run-query/issues/4)
* **test:** fix linebreaks ([78771fd](https://github.com/adobe/helix-run-query/commit/78771fd))


### Features

* **action:** turn action into a web action ([f41f212](https://github.com/adobe/helix-run-query/commit/f41f212))
* **auth:** add fastly authentication ([733d340](https://github.com/adobe/helix-run-query/commit/733d340))
* **index:** allow specifying query in path ([a20dfa3](https://github.com/adobe/helix-run-query/commit/a20dfa3))
* **index:** clean parameters ([8372db1](https://github.com/adobe/helix-run-query/commit/8372db1))
* **monitoring:** add helix-status library to enable monitoring ([ef72300](https://github.com/adobe/helix-run-query/commit/ef72300))
* **query:** run simple queries ([e834508](https://github.com/adobe/helix-run-query/commit/e834508))
* **query:** set the default dataset to the current service config id ([cb432a7](https://github.com/adobe/helix-run-query/commit/cb432a7))

## [1.2.1](https://github.com/adobe/helix-service/compare/v1.2.0...v1.2.1) (2019-09-03)


### Bug Fixes

* **package:** update @adobe/helix-status to version 4.2.1 ([f7ab47a](https://github.com/adobe/helix-service/commit/f7ab47a))

# [1.2.0](https://github.com/adobe/helix-service/compare/v1.1.2...v1.2.0) (2019-08-22)


### Features

* **monitoring:** add helix-status library to enable monitoring ([ef72300](https://github.com/adobe/helix-service/commit/ef72300))

## [1.1.2](https://github.com/adobe/helix-service/compare/v1.1.1...v1.1.2) (2019-07-24)


### Bug Fixes

* **package:** fix npm test script in order to provide information about failing tests ([80e7fe2](https://github.com/adobe/helix-service/commit/80e7fe2)), closes [#13](https://github.com/adobe/helix-service/issues/13)

## [1.1.1](https://github.com/adobe/helix-service/compare/v1.1.0...v1.1.1) (2019-06-17)


### Bug Fixes

* **package.json:** remove redundant istanbul dependency and use latest nyc ([b29f97d](https://github.com/adobe/helix-service/commit/b29f97d)), closes [#4](https://github.com/adobe/helix-service/issues/4)

# [1.1.0](https://github.com/adobe/helix-service/compare/v1.0.1...v1.1.0) (2019-06-12)


### Features

* **action:** turn action into a web action ([f41f212](https://github.com/adobe/helix-service/commit/f41f212))

## [1.0.1](https://github.com/adobe/helix-service/compare/v1.0.0...v1.0.1) (2019-06-12)


### Bug Fixes

* **build:** add missing dependency ([aa163d7](https://github.com/adobe/helix-service/commit/aa163d7))

# 1.0.0 (2019-06-12)


### Bug Fixes

* **build:** add wsk property for release tracking ([9e36a10](https://github.com/adobe/helix-service/commit/9e36a10))
* **build:** increase version number to get a release ([f04ab95](https://github.com/adobe/helix-service/commit/f04ab95))
