/*
 * Copyright © 2016 Cask Data, Inc.
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

import T from 'i18n-react';
// Initialize i18n
T.setTexts(require('../cdap/text/text-en.yaml'));

import Footer from 'components/Footer';
import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import WranglerHeader from 'wrangler/components/WranglerHeader';
import Wrangler from 'wrangler/components/Wrangler';

require('font-awesome-webpack!./styles/font-awesome.config.js');
require('./styles/lib-styles.less');
require('./styles/common.less');
require('./styles/main.less');

class WranglerParent extends Component {
  render () {
    return (
      <div>
        <WranglerHeader />
        <Wrangler />
      </div>
    );
  }
}

ReactDOM.render(
  <WranglerParent />,
  document.getElementById('app-container')
);

ReactDOM.render(
  <Footer />,
  document.getElementById('footer-container')
);
