/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import ReactDOM from 'react-dom';

require('../ui-utils/url-generator');
require('font-awesome-sass-loader!./styles/font-awesome.config.js');
require('./styles/lib-styles.scss');
require('./styles/common.scss');
require('./styles/main.scss');

import Loadable from 'react-loadable';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import Home from 'components/Home';
import Header from 'components/Header';
import Footer from 'components/Footer';
import ConnectionExample from 'components/ConnectionExample';
import cookie from 'react-cookie';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import T from 'i18n-react';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import RouteToNamespace from 'components/RouteToNamespace';
import Helmet from 'react-helmet';
import MyCDAPVersionApi from 'api/version.js';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import StatusFactory from 'services/StatusFactory';
import LoadingIndicator from 'components/LoadingIndicator';
import StatusAlertMessage from 'components/StatusAlertMessage';
import AuthorizationErrorMessage  from 'components/AuthorizationErrorMessage';
import Page404 from 'components/404';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import HttpExecutor from 'components/HttpExecutor';
import ErrorBoundary from 'components/ErrorBoundary';
const SampleTSXComponent = Loadable({
  loader: () => import (/* webpackChunkName: "SampleTSXComponent" */ 'components/SampleTSXComponent'),
  loading: LoadingSVGCentered
});

const Administration = Loadable({
  loader: () => import(/* webpackChunkName: "Administration" */ 'components/Administration'),
  loading: LoadingSVGCentered
});

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedNamespace : NamespaceStore.getState().selectedNamespace,
      version: '',
      authorizationFailed: false
    };
    this.eventEmitter = ee(ee);
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

    StatusFactory.startPollingForBackendStatus();
    this.eventEmitter.on(globalEvents.NONAMESPACE, () => {
      this.setState({
        authorizationFailed: true
      });
    });
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
      <BrowserRouter basename="/cdap">
        <div className="cdap-container">
          <Helmet title={T.translate('commons.cdap')} />
          <Header />
          <LoadingIndicator />
          <StatusAlertMessage />
          {
            this.state.authorizationFailed ?
              <AuthorizationErrorMessage />
            :
              <div className="container-fluid">
                <Switch>
                  <Route exact path="/" render={(props) => (
                    <ErrorBoundary>
                      <RouteToNamespace {...props} />
                    </ErrorBoundary>
                   )} />
                  <Route exact path="/notfound" render={(props) => (
                    <ErrorBoundary>
                      <Page404 {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route path="/administration" render={(props) => (
                    <ErrorBoundary>
                      <Administration {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route exact path="/ns" render={(props) => (
                    <ErrorBoundary>
                      <RouteToNamespace {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route path="/ns/:namespace" history={history} render={(props) => (
                    <ErrorBoundary>
                      <Home {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route path="/socket-example" render={(props) => (
                    <ErrorBoundary>
                      <ConnectionExample {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route exact path="/httpexecutor" render={(props) => (
                    <ErrorBoundary>
                      <HttpExecutor {...props} />
                    </ErrorBoundary>
                  )} />
                  <Route exact path="/ts-example" render={(props) => (
                    <ErrorBoundary>
                      <SampleTSXComponent {...props} />
                    </ErrorBoundary>
                  )} />
                  {
                    /*
                    Eventually handling 404 should move to the error boundary and all container components will have the error object.
                    */
                  }
                  <Route render={(props) => (
                    <ErrorBoundary>
                      <Page404 {...props} />
                    </ErrorBoundary>
                  )} />
                </Switch>
              </div>
          }
          <Footer />
        </div>
      </BrowserRouter>
    );
  }
}

CDAP.propTypes = {
  children: PropTypes.node
};

ReactDOM.render(
  <CDAP />,
  document.getElementById('app-container')
);
