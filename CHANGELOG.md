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
