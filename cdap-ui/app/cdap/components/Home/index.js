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
import React, {PropTypes, Component} from 'react';
import {Route, Switch} from 'react-router-dom';
import Page404 from 'components/404';
import EntityListView from 'components/EntityListView';
import AppDetailedView from 'components/AppDetailedView';
import DatasetDetailedView from 'components/DatasetDetailedView';
import StreamDetailedView from 'components/StreamDetailedView';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import RulesEngineHome from 'components/RulesEngineHome';
import DataPrepHome from 'components/DataPrepHome';
import DataPrepConnections from 'components/DataPrepConnections';
import DataPrepBrowser from 'components/DataPrep/DataPrepBrowser';
import {setActiveBrowser, setDatabaseProperties} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
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
