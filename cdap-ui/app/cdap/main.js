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
import Cookies from 'universal-cookie';
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
import { WINDOW_ON_FOCUS, WINDOW_ON_BLUR } from 'services/WindowManager';
import { MyNamespaceApi } from 'api/namespace';
import If from 'components/If';
import Page500 from 'components/500';
import LoadingSVG from 'components/LoadingSVG';
import { handlePageLevelError } from 'services/helpers';
import ToggleExperiment from 'components/Lab/ToggleExperiment';

const cookie = new Cookies();

const DAG = Loadable({
  loader: () => import(/* webpackChunkName: "DAG" */ 'components/DAG'),
  loading: LoadingSVGCentered,
});

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
    if (window.CDAP_CONFIG.securityEnabled && cookie.get('CDAP_Auth_Token')) {
      const token = `Bearer ${cookie.get('CDAP_Auth_Token')}`;

      operation.setContext({
        headers: {
          authorization: token,
          'Session-Token': SessionTokenStore.getState(),
          'X-Requested-With': 'XMLHttpRequest',
        },
      });
    } else {
      operation.setContext({
        headers: {
          'Session-Token': SessionTokenStore.getState(),
          'X-Requested-With': 'XMLHttpRequest',
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
      pageLevelError: false,
      isNamespaceFetchInFlight: false,
    };
    this.eventEmitter = ee(ee);
    this.eventEmitter.on(WINDOW_ON_FOCUS, this.onWindowFocus);
    this.eventEmitter.on(WINDOW_ON_BLUR, this.onWindowBlur);
  }

  componentWillMount() {
    this.fetchSessionTokenAndUpdateState().then(this.setUIState);
    this.eventEmitter.on(globalEvents.PAGE_LEVEL_ERROR, (err) => {
      if (err.reset === true) {
        this.setState({ pageLevelError: false });
      } else {
        this.setState({ pageLevelError: handlePageLevelError(err) });
      }
    });
  }

  componentWillUnmount() {
    this.eventEmitter.off(WINDOW_ON_FOCUS, this.onWindowFocus);
    this.eventEmitter.off(WINDOW_ON_BLUR, this.onWindowBlur);
    if (this.namespaceSub) {
      this.namespaceSub.unsubscribe();
    }
  }

  onWindowBlur = () => {
    StatusFactory.stopPollingForBackendStatus();
  };

  onWindowFocus = () => {
    this.fetchSessionTokenAndUpdateState().then(this.setUIState);
  };

  retrieveNamespace = () => {
    this.setState({ isNamespaceFetchInFlight: true });
    this.namespaceSub = MyNamespaceApi.list().subscribe(
      (res) => {
        if (res.length > 0) {
          NamespaceStore.dispatch({
            type: NamespaceActions.updateNamespaces,
            payload: {
              namespaces: res,
            },
          });
        } else {
          // TL;DR - This is emitted for Authorization in main.js
          // This means there is no namespace for the user to work on.
          // which indicates she/he have no authorization for any namesapce in the system.
          this.eventEmitter.emit(globalEvents.NONAMESPACE);
        }
        this.setState({ isNamespaceFetchInFlight: false });
      },
      (err) => {
        this.eventEmitter.emit(globalEvents.PAGE_LEVEL_ERROR, err);
        this.setState({ isNamespaceFetchInFlight: false });
      }
    );
  };

  setUIState = () => {
    StatusFactory.startPollingForBackendStatus();
    applyTheme();
    cookie.set('DEFAULT_UI', 'NEW', { path: '/' });
    if (window.CDAP_CONFIG.securityEnabled) {
      NamespaceStore.dispatch({
        type: NamespaceActions.updateUsername,
        payload: {
          username: cookie.get('CDAP_Auth_User') || '',
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
    if (this.state.loading) {
      this.setState({
        loading: false,
      });
    }
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
    const container = (
      <div className="container-fluid">
        <ErrorBoundary>
          <Switch>
            <Route exact path="/" render={(props) => <RouteToNamespace {...props} />} />
            <Route exact path="/notfound" render={(props) => <Page404 {...props} />} />
            <Route path="/administration" render={(props) => <Administration {...props} />} />
            <Route exact path="/ns" render={(props) => <RouteToNamespace {...props} />} />
            <Route
              path="/ns/:namespace"
              history={history}
              render={(props) => <Home {...props} />}
            />
            <Route exact path="/httpexecutor" render={(props) => <HttpExecutor {...props} />} />

            <Route
              exact
              path="/ts-example"
              render={(props) => {
                if (window.CDAP_CONFIG.cdap.mode !== 'development') {
                  return <Page404 {...props} />;
                }
                const SampleTSXComponent = Loadable({
                  loader: () =>
                    import(
                      /* webpackChunkName: "SampleTSXComponent" */ 'components/SampleTSXComponent'
                    ),
                  loading: LoadingSVGCentered,
                });
                return <SampleTSXComponent {...props} />;
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
                    import(
                      /* webpackChunkName: "MarkdownImplExample" */ 'components/Markdown/MarkdownImplExample'
                    ),
                  loading: LoadingSVGCentered,
                });
                return <MarkdownImpl {...props} />;
              }}
            />
            <Route
              exact
              path="/playground"
              render={(props) => (
                <ErrorBoundary>
                  <DAG {...props} />
                </ErrorBoundary>
              )}
            />
            <Route
              exact
              path="/schema"
              render={(props) => {
                const SchemaEditorDemo = Loadable({
                  loader: () =>
                    import(
                      /* webpackChunkName: "SchemaEditor" */ 'components/AbstractWidget/SchemaEditor/SchemaEditorDemo'
                    ),
                  loading: LoadingSVGCentered,
                });
                return (
                  <ToggleExperiment
                    name="schema-editor"
                    defaultComponent={<Page404 {...props} />}
                    experimentalComponent={<SchemaEditorDemo />}
                  />
                );
              }}
            />
            {/*
              Eventually handling 404 should move to the error boundary and all container components will have the error object.
            */}
            <Route render={(props) => <Page404 {...props} />} />
          </Switch>
        </ErrorBoundary>
      </div>
    );
    return (
      <Router history={history}>
        <ApolloProvider client={client}>
          <div className="cdap-container">
            <Helmet title={Theme.productName} />
            <AppHeader />
            <LoadingIndicator />
            <StatusAlertMessage />
            <If condition={this.state.isNamespaceFetchInFlight}>
              <div className="loading-svg">
                <LoadingSVG />
              </div>
            </If>
            <If condition={!this.state.isNamespaceFetchInFlight}>
              <If
                condition={this.state.pageLevelError && this.state.pageLevelError.errorCode === 404}
              >
                <Page404 message={this.state.pageLevelError.message} />
              </If>
              <If
                condition={this.state.pageLevelError && this.state.pageLevelError.errorCode !== 404}
              >
                <Page500
                  message={this.state.pageLevelError.message}
                  refreshFn={this.retrieveNamespace}
                />
              </If>

              <If condition={!this.state.pageLevelError}>
                {this.state.authorizationFailed ? <AuthorizationErrorMessage /> : container}
              </If>
            </If>
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
