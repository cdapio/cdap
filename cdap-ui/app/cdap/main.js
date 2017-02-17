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

import React, {Component, PropTypes} from 'react';
import ReactDOM from 'react-dom';

require('../ui-utils/url-generator');
require('font-awesome-sass-loader!./styles/font-awesome.config.js');
require('./styles/lib-styles.scss');
require('./styles/common.scss');
require('./styles/main.scss');

import Administration from 'components/Administration';
import Dashboard from 'components/Dashboard';
import Home from 'components/Home';
import Header from 'components/Header';
import Footer from 'components/Footer';
import SplashScreen from 'components/SplashScreen';
import ConnectionExample from 'components/ConnectionExample';
import Experimental from 'components/Experimental';
import cookie from 'react-cookie';
import Router from 'react-router/BrowserRouter';
import T from 'i18n-react';
import Match from 'react-router/Match';
import Miss from 'react-router/Miss';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import RouteToNamespace from 'components/RouteToNamespace';
import Helmet from 'react-helmet';
import SchemaEditor from 'components/SchemaEditor';
import MyCDAPVersionApi from 'api/version.js';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import StatusFactory from 'services/StatusFactory';
import LoadingIndicator from 'components/LoadingIndicator';
import StatusAlertMessage from 'components/StatusAlertMessage';
import Page404 from 'components/404';

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedNamespace : NamespaceStore.getState().selectedNamespace,
      version: ''
    };
  }

  componentWillMount() {
    cookie.save('DEFAULT_UI', 'NEW', {path: '/'});
    if (window.CDAP_CONFIG.securityEnabled) {
      NamespaceStore.dispatch({
        type: NamespaceActions.updateUsername,
        payload: {
          username: cookie.load('CDAP_Auth_User') || ''
        }
      });
    }

    StatusFactory.startPolling();

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

  render() {

    return (
      <Router basename="/cdap" history={history}>
        <div className="cdap-container">
          <Helmet
            title={T.translate('features.EntityListView.Title')}
          />
          <Header />
          <SplashScreen openVideo={this.openCaskVideo}/>
          <LoadingIndicator />
          <StatusAlertMessage />
          <div className="container-fluid">
            <Match exactly pattern="/" component={RouteToNamespace} />
            <Match exactly pattern="/notfound" component={Page404} />
            <Match exactly pattern="/administration" component={Administration} />
            <Match exactly pattern="/ns" component={RouteToNamespace} />
            <Match pattern="/ns/:namespace" history={history} component={Home} />
            <Match exactly pattern="/ns/:namespace/dashboard" component={Dashboard} />
            <Match pattern="/Experimental" component={Experimental} />
            <Match pattern="/socket-example" component={ConnectionExample} />
            <Match pattern="/schemaeditor" component={SchemaEditor} />
            <Miss component={Page404} />
          </div>
          <Footer version={this.state.version} />
        </div>
      </Router>
    );
  }
}

CDAP.propTypes = {
  children: React.PropTypes.node,
  params: PropTypes.object
};

ReactDOM.render(
  <CDAP />,
  document.getElementById('app-container')
);
