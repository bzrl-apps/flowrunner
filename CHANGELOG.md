## [1.0.0](https://github.com/bzrl-apps/flowrunner/compare/v0.11.0...v1.0.0) (2022-06-07)


### ⚠ BREAKING CHANGES

* **job:** Task result structure got changed when using with loop.
Loop task now has status, error & output fields as a normal task. So
array of item results is moved to output.
* **core:** flows accessing to exchanged message content such as data,
source, uuid etc. need to be changed. For ex: replace
`context.msg_id.data.event` instead of `context.data.event` when using
kafka source.

### 🚀 Features

* **builtin-git:** add condition for each action ([a31c213](https://github.com/bzrl-apps/flowrunner/commit/a31c21387306e7ae301e4d0b4de086db822c65de)), closes [#64](https://github.com/bzrl-apps/flowrunner/issues/64)
* **builtin-json-patch:** add condition to operation ([37e614a](https://github.com/bzrl-apps/flowrunner/commit/37e614afdb061de10cb579f622bcdcce94259f65)), closes [#65](https://github.com/bzrl-apps/flowrunner/issues/65)
* **builtin-lineinfile:** init plugin lineinfile ([0dadf0b](https://github.com/bzrl-apps/flowrunner/commit/0dadf0bfb7845f58995ad2699fae00b53c1cf916)), closes [#60](https://github.com/bzrl-apps/flowrunner/issues/60)
* **core:** add loop_tempo to temporise 2 executions in loop ([f1a3402](https://github.com/bzrl-apps/flowrunner/commit/f1a3402bd318167a7f6594741950404a6877dafb)), closes [#57](https://github.com/bzrl-apps/flowrunner/issues/57)
* **task:** allow register to access to context ([3e13c8f](https://github.com/bzrl-apps/flowrunner/commit/3e13c8f406b42ce28d5ed370485a0e7211a138a8)), closes [#67](https://github.com/bzrl-apps/flowrunner/issues/67)
* **task:** register global variables after task execution ([49c2b97](https://github.com/bzrl-apps/flowrunner/commit/49c2b972e46699b714e7b964023f0059dc5ad652)), closes [#62](https://github.com/bzrl-apps/flowrunner/issues/62)


### 🐛 Bug Fixes

* **builtin-git:** clone & authentication ([1b64e66](https://github.com/bzrl-apps/flowrunner/commit/1b64e663b27ae7a59409b3f2f8f1047bfde655b1))
* **builtin-git:** correctly convert value field from string to Value ([bc78081](https://github.com/bzrl-apps/flowrunner/commit/bc78081d555771e129f5336360dec0f10a268d7c))
* **builtin-git:** creation of local_dir when clonning ([fc3805c](https://github.com/bzrl-apps/flowrunner/commit/fc3805c054cb63b93c786cab2c7458baa0010799))
* **builtin-git:** detect changes and use add for git remove ([9418c97](https://github.com/bzrl-apps/flowrunner/commit/9418c97d98ef23612513cdf8f364cba8ad06a745))
* **builtin-git:** return error when no change found to add to commit ([9180bcb](https://github.com/bzrl-apps/flowrunner/commit/9180bcbb52316576b7124e73c1230e02d02f07d2))
* **builtin-git:** review params verifications ([f6acb02](https://github.com/bzrl-apps/flowrunner/commit/f6acb0237ef5fe256aa4225a4e7e86029eeddb81))
* **builtin-git:** several bugs found in add & push ([ad100ed](https://github.com/bzrl-apps/flowrunner/commit/ad100ed73279140c12d1030a7d276f8995e8937e))
* **core:** move exchanged message content to context.msg_id ([8f5d76a](https://github.com/bzrl-apps/flowrunner/commit/8f5d76a7cc2a7d99c4c8116cbfc17a184a6631ca)), closes [#63](https://github.com/bzrl-apps/flowrunner/issues/63)
* **job:** add status & error to task with loop ([c8656ac](https://github.com/bzrl-apps/flowrunner/commit/c8656acb077f1fbb4d3ebf4837c430eb73ee3538))
* **job:** do not stop at 1st error when looping ([8158e3d](https://github.com/bzrl-apps/flowrunner/commit/8158e3d71f1f6c998487421da36711f2b3113d38))
* **json-ops:** replacement of an array element ([49b7da4](https://github.com/bzrl-apps/flowrunner/commit/49b7da40467660f3f5bbe72e7e22155e14dd485f)), closes [#66](https://github.com/bzrl-apps/flowrunner/issues/66)
* remove username in the auth config ([23f4eea](https://github.com/bzrl-apps/flowrunner/commit/23f4eea8d6bde54251f56e72b07060f28fac8204))
* upgrade crates for security (dependabot alerts) ([7edde2d](https://github.com/bzrl-apps/flowrunner/commit/7edde2d1a2a209a152e5700883d230bf15859126))

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
