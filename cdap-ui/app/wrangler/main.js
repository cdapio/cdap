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

import Footer from 'components/Footer';
import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import Header from 'components/Header';
import {MyNamespaceApi} from 'api/namespace';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import RouteToNamespace from 'components/RouteToNamespace';
import Match from 'react-router/Match';
import Router from 'react-router/BrowserRouter';
import Home from 'wrangler/components/Home';
import MyCDAPVersionApi from 'api/version.js';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';

require('font-awesome-sass-loader!./styles/font-awesome.config.js');
require('./styles/lib-styles.scss');
require('./styles/common.scss');
require('./styles/main.scss');
require('../ui-utils/url-generator');

class WranglerParent extends Component {
  componentWillMount() {
    // Polls for namespace data
    MyNamespaceApi.pollList()
      .subscribe(
        (res) => {
          if (res.length > 0) {
            NamespaceStore.dispatch({
              type: NamespaceActions.updateNamespaces,
              payload: {
                namespaces : res
              }
            });
          } else {
            // To-Do: No namespaces returned ; throw error / redirect
          }
        }
      );

    if (!VersionStore.getState().version) {
      MyCDAPVersionApi.get().subscribe((res) => {
        this.setState({ version : res.version });
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version
          }
        });
      });
    }
  }

  render () {
    return (

      <Router basename="/wrangler" history={history}>
        <div className="cdap-container">

          <Header />

          <div className="container-fluid">
            <Match exactly pattern="/" component={RouteToNamespace} />
            <Match exactly pattern="/ns" component={RouteToNamespace} />
            <Match exactly pattern="/ns/:namespace" history={history} component={Home} />
          </div>
        </div>
      </Router>
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
