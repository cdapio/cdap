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
require('font-awesome-webpack!./styles/font-awesome.config.js');
require('./styles/lib-styles.less');
require('./styles/common.less');
require('./styles/main.less');

import Management from 'components/Management';
import Dashboard from 'components/Dashboard';
import Home from 'components/Home';
import CdapHeader from 'components/CdapHeader';
import Footer from 'components/Footer';
import SplashScreen from 'components/SplashScreen';
import ConnectionExample from 'components/ConnectionExample';
import Experimental from 'components/Experimental';
import cookie from 'react-cookie';
import {MyNamespaceApi} from 'api/namespace';
import Router from 'react-router/BrowserRouter';
import T from 'i18n-react';
import Match from 'react-router/Match';
import Miss from 'react-router/Miss';
import Store from 'services/store/store';
import CaskVideoModal from 'components/CaskVideoModal';
import RouteToNamespace from 'components/RouteToNamespace';
import Helmet from 'react-helmet';

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.version = '4.0.0';
    this.closeCaskVideo = this.closeCaskVideo.bind(this);
    this.openCaskVideo = this.openCaskVideo.bind(this);
    this.state = {
      selectedNamespace : Store.getState().selectedNamespace,
      videoOpen : false
    };
  }

  openCaskVideo(){
    this.setState({
      videoOpen : true
    });
  }

  closeCaskVideo(){
    this.setState({
      videoOpen : false
    });
  }

  componentWillMount(){
    // Polls for namespace data
    MyNamespaceApi.pollList()
      .subscribe((res) => {
        if (res.length > 0){
          Store.dispatch({
            type: 'UPDATE_NAMESPACES',
            payload: {
              namespaces : res
            }
          });
        } else {
          //To-Do: No namespaces returned ; throw error / redirect
        }
      });
  }

  render() {
    if ( window.CDAP_CONFIG.securityEnabled && !cookie.load('CDAP_Auth_Token')) {
      //authentication failed ; redirect to another page
      window.location.href = window.getAbsUIUrl({
        uiApp: 'login',
        redirectUrl: location.href,
        clientId: 'cdap'
      });
      return null;
    }

    return (
      <Router basename="/cask-cdap" history={history}>
        <div className="cdap-container">
          <Helmet
            title={T.translate('features.Home.Title')}
          />
          <CdapHeader />
          <SplashScreen openVideo={this.openCaskVideo}/>
          <CaskVideoModal isOpen={this.state.videoOpen} onCloseHandler={this.closeCaskVideo}/>
          <div className="container-fluid">
            <Match exactly pattern="/" component={RouteToNamespace} />
            <Match exactly pattern="/notfound" component={Missed} />
            <Match exactly pattern="/management" component={Management} />
            <Match exactly pattern="/ns/:namespace" history={history} component={Home} />
            <Match exactly pattern="/ns/:namespace/dashboard" component={Dashboard} />
            <Match pattern="/Experimental" component={Experimental} />
            <Match pattern="/socket-example" component={ConnectionExample} />
            <Miss component={Missed} />
          </div>
          <Footer version={this.version} />
        </div>
      </Router>
    );
  }
}

CDAP.propTypes = {
  children: React.PropTypes.node,
  params: PropTypes.object
};

function Missed({ location }) {
  return (
    <div>
      <h2>404 - Page Not Found</h2>
      <p>Page {location.pathname} not found</p>
    </div>
  );
}

Missed.propTypes = {
  location : PropTypes.object
};

ReactDOM.render(
  <CDAP />,
  document.getElementById('app-container')
);
