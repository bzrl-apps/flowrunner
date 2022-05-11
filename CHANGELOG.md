## [0.11.0](https://github.com/bzrl-apps/flowrunner/compare/v0.10.0...v0.11.0) (2022-05-11)


### 🐛 Bug Fixes

* **builtin-dnsquery:** loop query response's answers instead of the 1st ([7b5935f](https://github.com/bzrl-apps/flowrunner/commit/7b5935f04be902ec620e095b2a08b7d7383d6379))
* **core:** only sources with sink capability can receive job msgs ([5b89b06](https://github.com/bzrl-apps/flowrunner/commit/5b89b063a6bfedd1ff8f8dbbae875a47fe5f2703))
* **core:** set job_parallel option to true before parsing ([2b8b7ee](https://github.com/bzrl-apps/flowrunner/commit/2b8b7ee300faa8e134c424b18144ed2466bf3efe))
* **core:** use arc & mutex to store map of job results in cache ([7bc741e](https://github.com/bzrl-apps/flowrunner/commit/7bc741ef4f0e1b905a54b9f42e2553b171514500))


### 🚀 Features

* **builtin-dnsquery:** init dnsquery plugin ([4499d0f](https://github.com/bzrl-apps/flowrunner/commit/4499d0f97007777425d7364cfd8a2747c76eaf22)), closes [#49](https://github.com/bzrl-apps/flowrunner/issues/49)
* **builtin-git:** init git plugin ([9f4c599](https://github.com/bzrl-apps/flowrunner/commit/9f4c599d4db411457f069dc5672ea74faf42f03a)), closes [#50](https://github.com/bzrl-apps/flowrunner/issues/50)
* **builtin-httpserver:** implement handlers for GET/POST/PUT/DELETE ([6c65ac4](https://github.com/bzrl-apps/flowrunner/commit/6c65ac49aa992de7e806b8dcd9bda9769fdfecaf)), closes [#47](https://github.com/bzrl-apps/flowrunner/issues/47)
* **builtin-httpserver:** init plugin http server ([6c328c9](https://github.com/bzrl-apps/flowrunner/commit/6c328c9a3c8318e6a51eebaba0d155d6bad31d68))
* **builtin-template-tera:** init template rendering with Tera ([37ddf56](https://github.com/bzrl-apps/flowrunner/commit/37ddf56a91f2bf4e3e4269b8c8b596586a6bb9db)), closes [#51](https://github.com/bzrl-apps/flowrunner/issues/51)
* **core:** add UUID to FlowMessage::JsonWithSender to trace msg exchanges ([ce7be79](https://github.com/bzrl-apps/flowrunner/commit/ce7be7991882f64828780637677cc26e91740cf2))
* **core:** allow a source to receive job's messages as a sink ([038d004](https://github.com/bzrl-apps/flowrunner/commit/038d004b75a2bbd2bf1eb90f4d50e66e2c8d34d9))
* **core:** give job access to the cache in sequential mode via context ([9da1186](https://github.com/bzrl-apps/flowrunner/commit/9da11861756476d51a4cd3be51d0958592d6489b)), closes [#55](https://github.com/bzrl-apps/flowrunner/issues/55)
* **core:** handle sequential job execution with Moka cache ([8acbb9e](https://github.com/bzrl-apps/flowrunner/commit/8acbb9e81422e7689671fba9d087e7bdae3e081f))
* **core:** set timeout for checking the execution of dependend jobs ([b161464](https://github.com/bzrl-apps/flowrunner/commit/b161464b98bce0ab1aaca707a37a3ed5d7146823))
* **cron:** add a cron server ([ffa20ca](https://github.com/bzrl-apps/flowrunner/commit/ffa20ca576b2daa222ba1b3467be2781d64e408f)), closes [#12](https://github.com/bzrl-apps/flowrunner/issues/12)
* **flow:** add option to run jobs sequentially ([ff0b24f](https://github.com/bzrl-apps/flowrunner/commit/ff0b24fbe4eeb5c134ef966ade0bd7abf93f18bf)), closes [#28](https://github.com/bzrl-apps/flowrunner/issues/28)

## [0.10.0](https://github.com/bzrl-apps/flowrunner/compare/v0.9.0...v0.10.0) (2022-04-19)


### 🚀 Features

* **builtin-pgql-tokio:** handle TLS configuration ([6e6336b](https://github.com/bzrl-apps/flowrunner/commit/6e6336b44c90ce466db8c9b128b8931fa11a8436)), closes [#42](https://github.com/bzrl-apps/flowrunner/issues/42)

## [0.9.0](https://github.com/bzrl-apps/flowrunner/compare/v0.8.2...v0.9.0) (2022-04-18)


### 🚀 Features

* **builtin-tokio-pgql:** init new plugin for tokio postgres ([515c29e](https://github.com/bzrl-apps/flowrunner/commit/515c29ea9f62b72d610ee84b29ccaf882c61fb40)), closes [#41](https://github.com/bzrl-apps/flowrunner/issues/41)


### 🐛 Bug Fixes

* **builtin-pgql-sqlx:** rename builtin-pgql to builtin-pgql-sqlx ([b52afbb](https://github.com/bzrl-apps/flowrunner/commit/b52afbbc42d9a68d37c049920839671ea2d1f449))

### [0.8.2](https://github.com/bzrl-apps/flowrunner/compare/v0.8.1...v0.8.2) (2022-04-15)


### 🐛 Bug Fixes

* try to fix prepared stmt by putting deallcate before each query ([fdcf03c](https://github.com/bzrl-apps/flowrunner/commit/fdcf03cfb1136fcfa5a1aa7f21228ae677e3b4a2))

### [0.8.1](https://github.com/bzrl-apps/flowrunner/compare/v0.8.0...v0.8.1) (2022-04-15)


### 🐛 Bug Fixes

* try to fix prepared statement issue with pgbouncer ([63c6408](https://github.com/bzrl-apps/flowrunner/commit/63c6408153cb1fe1d14beb83a9db9112b3ec9e79))

## [0.8.0](https://github.com/bzrl-apps/flowrunner/compare/v0.7.0...v0.8.0) (2022-04-14)


### 🚀 Features

* **core:** add generate_uuid() to Tera ([7c97905](https://github.com/bzrl-apps/flowrunner/commit/7c97905a2085362ccdc719340d8f842ded1c3a7a)), closes [#40](https://github.com/bzrl-apps/flowrunner/issues/40)

## [0.7.0](https://github.com/bzrl-apps/flowrunner/compare/v0.6.0...v0.7.0) (2022-04-13)


### 🚀 Features

* **job:** add loop_index to track item index in the loop ([164552f](https://github.com/bzrl-apps/flowrunner/commit/164552f04d16ee08ffcc6425a6afbb363e897dfd)), closes [#38](https://github.com/bzrl-apps/flowrunner/issues/38)


### 🐛 Bug Fixes

* **builtin-json-patch:** errors while using with template rendering ([7a261b8](https://github.com/bzrl-apps/flowrunner/commit/7a261b8a235548118dbd341e559b2647ab4f7cdf)), closes [#37](https://github.com/bzrl-apps/flowrunner/issues/37)
* **builtin-pgql:** add option to deallocating prep statements ([42b06ec](https://github.com/bzrl-apps/flowrunner/commit/42b06ece2c3125c34c95188138beb6a16a65d18b)), closes [#35](https://github.com/bzrl-apps/flowrunner/issues/35)
* **builtin-pgql:** default value for cond & fetch ([db14114](https://github.com/bzrl-apps/flowrunner/commit/db141147382dd730546c182fdb120a714b7956af)), closes [#32](https://github.com/bzrl-apps/flowrunner/issues/32) [#33](https://github.com/bzrl-apps/flowrunner/issues/33)
* **json-ops:** correct wrong new value type to replace ([58eeac7](https://github.com/bzrl-apps/flowrunner/commit/58eeac73dfa189090d60c1ae0630b2116f07b7a8)), closes [#39](https://github.com/bzrl-apps/flowrunner/issues/39)

## [0.6.0](https://github.com/bzrl-apps/flowrunner/compare/v0.5.0...v0.6.0) (2022-04-03)


### 🚀 Features

* **builtin-kafka-producer:** add if condition for each msg to send ([357bf58](https://github.com/bzrl-apps/flowrunner/commit/357bf5842a4d302990143f72722c9be09fa0b2d3)), closes [#31](https://github.com/bzrl-apps/flowrunner/issues/31)


### 🐛 Bug Fixes

* **builtin-pgql:** use runtime block_on() instead of enter() ([8383f50](https://github.com/bzrl-apps/flowrunner/commit/8383f503dcd3d6c585a5f431d40626d28e48abdf))
* **job:** reinitialize job's result for each received msg ([e816049](https://github.com/bzrl-apps/flowrunner/commit/e81604929ee9063b60743e40ed665bbb79b98c4b)), closes [#34](https://github.com/bzrl-apps/flowrunner/issues/34)
