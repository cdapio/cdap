/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import AppHeader from 'components/AppHeader';
import Footer from 'components/Footer';
import cookie from 'react-cookie';
import { Router, Route, Switch } from 'react-router-dom';
import history from 'services/history';
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
import AuthorizationErrorMessage from 'components/AuthorizationErrorMessage';
import Page404 from 'components/404';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import HttpExecutor from 'components/HttpExecutor';
import { applyTheme } from 'services/ThemeHelper';
import ErrorBoundary from 'components/ErrorBoundary';
import { Theme } from 'services/ThemeHelper';
import AuthRefresher from 'components/AuthRefresher';
import ThemeWrapper from 'components/ThemeWrapper';
import './globals';
import { ApolloProvider } from 'react-apollo';
import ApolloClient from 'apollo-boost';
import { IntrospectionFragmentMatcher, InMemoryCache } from 'apollo-cache-inmemory';
// See ./graphql/fragements/README.md
import introspectionQueryResultData from '../../graphql/fragments/fragmentTypes.json';
import SessionTokenStore, { fetchSessionToken } from 'services/SessionTokenStore';
const Administration = Loadable({
  loader: () => import(/* webpackChunkName: "Administration" */ 'components/Administration'),
  loading: LoadingSVGCentered,
});

const fragmentMatcher = new IntrospectionFragmentMatcher({
  introspectionQueryResultData,
});

const client = new ApolloClient({
  uri: '/graphql',
  cache: new InMemoryCache({ fragmentMatcher }),
  request: (operation) => {
    if (window.CDAP_CONFIG.securityEnabled && cookie.load('CDAP_Auth_Token')) {
      const token = `Bearer ${cookie.load('CDAP_Auth_Token')}`;

      operation.setContext({
        headers: {
          authorization: token,
          'Session-Token': SessionTokenStore.getState(),
        },
      });
    }
  },
});

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedNamespace: NamespaceStore.getState().selectedNamespace,
      version: '',
      authorizationFailed: false,
      loading: true,
    };
    this.eventEmitter = ee(ee);
  }

  componentWillMount() {
    this.fetchSessionTokenAndUpdateState().then(this.setUIState);
  }

  setUIState = () => {
    StatusFactory.startPollingForBackendStatus();
    applyTheme();
    cookie.save('DEFAULT_UI', 'NEW', { path: '/' });
    if (window.CDAP_CONFIG.securityEnabled) {
      NamespaceStore.dispatch({
        type: NamespaceActions.updateUsername,
        payload: {
          username: cookie.load('CDAP_Auth_User') || '',
        },
      });
    }

    this.eventEmitter.on(globalEvents.NONAMESPACE, () => {
      this.setState({
        authorizationFailed: true,
      });
    });
    if (!VersionStore.getState().version) {
      MyCDAPVersionApi.get().subscribe((res) => {
        this.setState({ version: res.version });
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version,
          },
        });
      });
    }
  };

  async fetchSessionTokenAndUpdateState() {
    try {
      await fetchSessionToken();
    } catch (e) {
      console.log('Fetching session token failed.');
    }
    this.setState({
      loading: false,
    });
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="cdap-container">
          <Helmet title={Theme.productName} />
          <LoadingSVGCentered />
          <LoadingIndicator />
        </div>
      );
    }
    return (
      <Router history={history}>
        <ApolloProvider client={client}>
          <div className="cdap-container">
            <Helmet title={Theme.productName} />
            <AppHeader />
            <LoadingIndicator />
            <StatusAlertMessage />
            {this.state.authorizationFailed ? (
              <AuthorizationErrorMessage />
            ) : (
              <div className="container-fluid">
                <Switch>
                  <Route
                    exact
                    path="/"
                    render={(props) => (
                      <ErrorBoundary>
                        <RouteToNamespace {...props} />
                      </ErrorBoundary>
                    )}
                  />
                  <Route
                    exact
                    path="/notfound"
                    render={(props) => (
                      <ErrorBoundary>
                        <Page404 {...props} />
                      </ErrorBoundary>
                    )}
                  />
                  <Route
                    path="/administration"
                    render={(props) => (
                      <ErrorBoundary>
                        <Administration {...props} />
                      </ErrorBoundary>
                    )}
                  />
                  <Route
                    exact
                    path="/ns"
                    render={(props) => (
                      <ErrorBoundary>
                        <RouteToNamespace {...props} />
                      </ErrorBoundary>
                    )}
                  />
                  <Route
                    path="/ns/:namespace"
                    history={history}
                    render={(props) => (
                      <ErrorBoundary>
                        <Home {...props} />
                      </ErrorBoundary>
                    )}
                  />
                  <Route
                    exact
                    path="/httpexecutor"
                    render={(props) => (
                      <ErrorBoundary>
                        <HttpExecutor {...props} />
                      </ErrorBoundary>
                    )}
                  />

                  <Route
                    exact
                    path="/ts-example"
                    render={(props) => {
                      if (window.CDAP_CONFIG.cdap.mode !== 'development') {
                        return <Page404 {...props} />;
                      }
                      const SampleTSXComponent = Loadable({
                        loader: () =>
                          import(/* webpackChunkName: "SampleTSXComponent" */ 'components/SampleTSXComponent'),
                        loading: LoadingSVGCentered,
                      });
                      return (
                        <ErrorBoundary>
                          <SampleTSXComponent {...props} />
                        </ErrorBoundary>
                      );
                    }}
                  />
                  <Route
                    exact
                    path="/markdownexperiment"
                    render={(props) => {
                      if (window.CDAP_CONFIG.cdap.mode !== 'development') {
                        return <Page404 {...props} />;
                      }
                      const MarkdownImpl = Loadable({
                        loader: () =>
                          import(/* webpackChunkName: "MarkdownImplExample" */ 'components/Markdown/MarkdownImplExample'),
                        loading: LoadingSVGCentered,
                      });
                      return (
                        <ErrorBoundary>
                          <MarkdownImpl {...props} />
                        </ErrorBoundary>
                      );
                    }}
                  />
                  <Route
                    exact
                    path="/selectionbox"
                    render={(props) => {
                      if (window.CDAP_CONFIG.cdap.mode !== 'development') {
                        return <Page404 {...props} />;
                      }
                      const SelectionBox = Loadable({
                        loader: () =>
                          import(/* webpackChunkName: "SelectionBox" */ 'components/SelectionBox/SelectionBoxWrapper'),
                        loading: LoadingSVGCentered,
                      });
                      return (
                        <ErrorBoundary>
                          <SelectionBox {...props} />
                        </ErrorBoundary>
                      );
                    }}
                  />
                  <Route
                    exact
                    path="/contextmenu"
                    render={(props) => {
                      if (window.CDAP_CONFIG.cdap.mode !== 'development') {
                        return <Page404 {...props} />;
                      }
                      const ContextMenu = Loadable({
                        loader: () =>
                          import(/* webpackChunkName: "ContextMenu" */ 'components/ContextMenu'),
                        loading: LoadingSVGCentered,
                      });
                      return (
                        <ErrorBoundary>
                          <ContextMenu {...props} />
                        </ErrorBoundary>
                      );
                    }}
                  />
                  {/*
                    Eventually handling 404 should move to the error boundary and all container components will have the error object.
                  */}
                  <Route
                    render={(props) => (
                      <ErrorBoundary>
                        <Page404 {...props} />
                      </ErrorBoundary>
                    )}
                  />
                </Switch>
              </div>
            )}
            <Footer />
            <AuthRefresher />
          </div>
        </ApolloProvider>
      </Router>
    );
  }
}

CDAP.propTypes = {
  children: PropTypes.node,
};

ReactDOM.render(<ThemeWrapper render={() => <CDAP />} />, document.getElementById('app-container'));
