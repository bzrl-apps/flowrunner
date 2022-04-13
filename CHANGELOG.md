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
