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
import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {Route, Switch} from 'react-router-dom';
import Page404 from 'components/404';
import EntityListView from 'components/EntityListView';
import Loadable from 'react-loadable';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import {setActiveBrowser, setDatabaseProperties} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import OpsDashboard from 'components/OpsDashboard';
require('./Home.scss');

const DataPrepBrowser = Loadable({
  loader: () => import(/* webpackChunkName: "DataPrepBrowser" */ 'components/DataPrep/DataPrepBrowser'),
  loading: LoadingSVGCentered
});
const DataPrepConnections = Loadable({
  loader: () => import(/* webpackChunkName: "DataPrepConnections" */ 'components/DataPrepConnections'),
  loading: LoadingSVGCentered
});
const DataPrepHome = Loadable({
  loader: () => import(/* webpackChunkName: "DataPrepHome" */ 'components/DataPrepHome'),
  loading: LoadingSVGCentered
});
const RulesEngineHome = Loadable({
  loader: () => import(/* webpackChunkName: "RulesEngineHome" */ 'components/RulesEngineHome'),
  loading: LoadingSVGCentered
});
const DatasetDetailedView = Loadable({
  loader: () => import(/* webpackChunkName: "DatasetDetailedView" */ 'components/DatasetDetailedView'),
  loading: LoadingSVGCentered
});
const StreamDetailedView = Loadable({
  loader: () => import(/* webpackChunkName: "StreamDetailedView" */ 'components/StreamDetailedView'),
  loading: LoadingSVGCentered
});
const AppDetailedView = Loadable({
  loader: () => import(/* webpackChunkName: "AppDetailedView" */ 'components/AppDetailedView'),
  loading: LoadingSVGCentered
});
const Experiments = Loadable({
  loader: () => import(/* webpackChunkName: "Experiments" */ 'components/Experiments'),
  loading: LoadingSVGCentered
});
const NamespaceDetails = Loadable({
  loader: () => import(/* webpackChunkName: "NamespaceDetails" */ 'components/NamespaceDetails'),
  loading: LoadingSVGCentered
});

const ProfileCreateView = Loadable({
  loader: () => import(/* webpackChunkName: "Experiments" */ 'components/Cloud/Profiles/CreateView'),
  loading: LoadingSVGCentered
});

export default class Home extends Component {
  componentWillMount() {
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace: this.props.match.params.namespace
      }
    });
  }
  render() {
    return (
      <div>
        <Switch>
          <Route exact path="/ns/:namespace" component={EntityListView} />
          <Route path="/ns/:namespace/apps/:appId" component={AppDetailedView} />
          <Route path="/ns/:namespace/datasets/:datasetId" component={DatasetDetailedView} />
          <Route path="/ns/:namespace/streams/:streamId" component={StreamDetailedView} />
          <Route exact path="/ns/:namespace/rulesengine" component={RulesEngineHome} />
          <Route exact path="/ns/:namespace/dataprep" component={DataPrepHome} />
          <Route exact path="/ns/:namespace/dataprep/:workspaceId" component={DataPrepHome} />
          <Route path="/ns/:namespace/databasebrowser" render={() => {
            setActiveBrowser({ name: 'database' });
            setDatabaseProperties({
              properties: {
                connectionString: 'jdbc:mysql://localhost:3306/test',
                userName: 'root',
                password: 'root',
                databasename: 'test'
              }
            });
            return (
              <DataPrepBrowser />
            );
          }} />
          <Route path="/ns/:namespace/connections" component={DataPrepConnections} />
          <Route path="/ns/:namespace/experiments" component={Experiments} />
          <Route exact path="/ns/:namespace/operations" component={OpsDashboard} />
          <Route exact path="/ns/:namespace/details" component={NamespaceDetails} />
          <Route exact path="/ns/:namespace/create-profile" component={ProfileCreateView} />
          <Route component={Page404} />
        </Switch>
      </div>
    );
  }
}

Home.propTypes = {
  params: PropTypes.shape({
    namespace : PropTypes.string
  }),
  match: PropTypes.object
};
