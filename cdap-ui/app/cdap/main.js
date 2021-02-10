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

import { hot } from 'react-hot-loader/root';
import 'react-hot-loader/patch';
import './globals';

import { InMemoryCache, IntrospectionFragmentMatcher } from 'apollo-cache-inmemory';
import React, { Component } from 'react';
import { Route, Router, Switch } from 'react-router-dom';
import SessionTokenStore, { fetchSessionToken } from 'services/SessionTokenStore';
import { Theme, applyTheme } from 'services/ThemeHelper';
import { WINDOW_ON_BLUR, WINDOW_ON_FOCUS } from 'services/WindowManager';

import ApolloClient from 'apollo-boost';
import { ApolloProvider } from 'react-apollo';
import AppHeader from 'components/AppHeader';
import AuthRefresher from 'components/AuthRefresher';
import AuthorizationErrorMessage from 'components/AuthorizationErrorMessage';
import Cookies from 'universal-cookie';
import ErrorBoundary from 'components/ErrorBoundary';
import Footer from 'components/Footer';
import Helmet from 'react-helmet';
import Home from 'components/Home';
import HttpExecutor from 'components/HttpExecutor';
import If from 'components/If';
import Loadable from 'react-loadable';
import LoadingIndicator from 'components/LoadingIndicator';
import LoadingSVG from 'components/LoadingSVG';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import MyCDAPVersionApi from 'api/version.js';
import { MyNamespaceApi } from 'api/namespace';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import NamespaceStore from 'services/NamespaceStore';
import Page404 from 'components/404';
import Page500 from 'components/500';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import RouteToNamespace from 'components/RouteToNamespace';
import StatusAlertMessage from 'components/StatusAlertMessage';
import StatusFactory from 'services/StatusFactory';
// Initialize i18n
import T from 'i18n-react';
import ThemeWrapper from 'components/ThemeWrapper';
import ToggleExperiment from 'components/Lab/ToggleExperiment';
import VersionActions from 'services/VersionStore/VersionActions';
import VersionStore from 'services/VersionStore';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import { handlePageLevelError, objectQuery, setupExperiments } from 'services/helpers';
import history from 'services/history';
// See ./graphql/fragements/README.md
import introspectionQueryResultData from '../../graphql/fragments/fragmentTypes.json';

require('../ui-utils/url-generator');
require('font-awesome-sass-loader!./styles/font-awesome.config.js');
require('./styles/lib-styles.scss');
require('./styles/common.scss');
require('./styles/main.scss');
T.setTexts(require('./text/text-en.yaml'));

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

const Lab = Loadable({
  loader: () => import(/* webpackChunkMame: "Lab" */ 'components/Lab'),
  loading: LoadingSVGCentered,
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
    setupExperiments();
  }

  componentWillMount() {
    this.fetchSessionTokenAndUpdateState().then(this.setUIState);
    this.eventEmitter.on(globalEvents.PAGE_LEVEL_ERROR, (err) => {
      if (err.reset === true) {
        this.setState({ pageLevelError: false });
      } else {
        this.setState({ pageLevelError: handlePageLevelError(err), loading: false });
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
        loading: false,
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
                    import(/* webpackChunkName: "MarkdownImplExample" */ 'components/Markdown'),
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
            <Route path="/lab" component={Lab} />
            <Route
              exact
              path="/lab-experiment-test"
              render={(props) => {
                if (!window.parent.Cypress) {
                  return <Page404 {...props} />;
                }
                const LabExperimentTestComp = Loadable({
                  loader: () =>
                    import(
                      /* webpackChunkName: "LabExperimentTest" */ 'components/Lab/LabExperimentTest'
                    ),
                  loading: LoadingSVGCentered,
                });
                return <LabExperimentTestComp {...props} />;
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
    const isNamespaceNotFound = objectQuery(this.state, 'pageLevelError', 'errorCode') === 404;
    const isUserUnAuthorizedForNamespace =
      objectQuery(this.state, 'pageLevelError', 'errorCode') === 403;
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
              <If condition={isNamespaceNotFound}>
                <Page404 message={this.state.pageLevelError.message} />
              </If>
              {/** We show authorization failure message when the specific namespace API returns 403 */}
              <If condition={isUserUnAuthorizedForNamespace}>
                <AuthorizationErrorMessage message={this.state.pageLevelError.message} />
              </If>
              <If
                condition={
                  !isUserUnAuthorizedForNamespace &&
                  !isNamespaceNotFound &&
                  this.state.pageLevelError
                }
              >
                <Page500
                  message={this.state.pageLevelError.message}
                  refreshFn={this.retrieveNamespace}
                />
              </If>
              {/**
               * We show authorization failure message when the backend return empty
               * namespace. This indicates the user has access to no namespace.
               */}
              <If condition={!this.state.pageLevelError}>
                {this.state.authorizationFailed ? (
                  <AuthorizationErrorMessage message={this.state.pageLevelError.message} />
                ) : (
                  container
                )}
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

const RootComp = hot(() => {
  return <ThemeWrapper render={() => <CDAP />} />;
});

ReactDOM.render(<RootComp />, document.getElementById('app-container'));
