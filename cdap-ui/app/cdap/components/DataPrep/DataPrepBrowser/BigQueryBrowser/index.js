/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import { Provider } from 'react-redux';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import DisplaySwitch from 'components/DataPrep/DataPrepBrowser/BigQueryBrowser/DisplaySwitch';
import DatasetList from 'components/DataPrep/DataPrepBrowser/BigQueryBrowser/DatasetList';
import TableList from 'components/DataPrep/DataPrepBrowser/BigQueryBrowser/TableList';
import { Route, Switch } from 'react-router-dom';
import DataPrepBrowserPageTitle from 'components/DataPrep/DataPrepBrowser/PageTitle';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';

require('./BigQueryBrowser.scss');

const PREFIX = `features.DataPrep.DataPrepBrowser.BigQueryBrowser`;

export default class BiqQueryBrowser extends Component {
  static propTypes = {
    toggle: PropTypes.func,
    onWorkspaceCreate: PropTypes.func,
    enableRouting: PropTypes.bool,
    scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    showPanelToggle: PropTypes.bool,
  };

  static defaultProps = {
    enableRouting: true,
  };

  render() {
    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="bigquery-browser">
          {this.props.enableRouting ? (
            <DataPrepBrowserPageTitle
              browserI18NName="BigQueryBrowser"
              browserStateName="bigquery"
              locationToPathInState={['datasetId']}
            />
          ) : null}

          <DataprepBrowserTopPanel
            allowSidePanelToggle={true}
            toggle={this.props.toggle}
            browserTitle={T.translate(`${PREFIX}.title`)}
            showPanelToggle={this.props.showPanelToggle}
          />
          {this.props.enableRouting ? (
            <Switch>
              <Route
                exact
                path="/ns/:namespace/connections/bigquery/:connectionId"
                component={DatasetList}
              />
              <Route
                exact
                path="/ns/:namespace/connections/bigquery/:connectionId/datasets/:datasetId"
                render={(routeProps) => <TableList {...routeProps} scope={this.props.scope} />}
              />
            </Switch>
          ) : (
            <DisplaySwitch
              onWorkspaceCreate={this.props.onWorkspaceCreate}
              scope={this.props.scope}
            />
          )}
        </div>
      </Provider>
    );
  }
}
