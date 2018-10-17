/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/
import { configure, setAddon } from '@storybook/react';

require('font-awesome-sass-loader!../app/cdap/styles/font-awesome.config.js');
require('../app/cdap/styles/lib-styles.scss');
require('../app/cdap/styles/common.scss');
require('../app/cdap/styles/main.scss');
require('./stories.global.scss');
require('../app/cdap/styles/bootstrap_4_patch.scss');

// automatically import all files ending in *.stories.js
const req = require.context('../app/cdap/components/', true, /.stories.tsx$/);
function loadStories() {
  require('./Welcome');
  req.keys().forEach(filename => req(filename));
}

configure(loadStories, module);
