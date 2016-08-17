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

import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import { Router, Route, browserHistory, IndexRedirect } from 'react-router';

require('../ui-utils/url-generator');
require('font-awesome-webpack!./styles/font-awesome.config.js');

import Management from './components/Management';
import Dashboard from './components/Dashboard';
import Home from './components/Home';
import Header from './components/Header';
import Footer from './components/Footer';
import SplashScreen from './components/SplashScreen';

require('./styles/lib-styles.less');
require('./styles/common.less');

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.version = '4.0.0';
  }
  render() {
    return (
      <div>
        <Header />
        <SplashScreen />
        <div className="container-fluid">
          {this.props.children}
        </div>
        <Footer version={this.version}/>
      </div>
    );
  }
}
CDAP.propTypes = {
  children: React.PropTypes.node
};

ReactDOM.render(
  <Router history={browserHistory}>
    <Route path="/" component={CDAP}>
      <IndexRedirect to="/home" />
      <Route name="home" path="home" component={Home}/>
      <Route name="dashboard" path="dashboard" component={Dashboard}/>
      <Route path="management" component={Management}/>
    </Route>
  </Router>,
  document.getElementById('app-container')
);
