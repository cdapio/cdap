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

import * as React from 'react';
import T from 'i18n-react';
import { Provider } from 'react-redux';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import SpannerDisplaySwitch from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/DisplaySwitch';
import SpannerInstanceList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/InstanceList';
import SpannerDatabaseList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/DatabaseList';
import SpannerTableList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/TableList';
import { Route, Switch } from 'react-router-dom';
import DataPrepBrowserPageTitle from 'components/DataPrep/DataPrepBrowser/PageTitle';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';

require('./SpannerBrowser.scss');

const PREFIX = `features.DataPrep.DataPrepBrowser.SpannerBrowser`;

interface ISpannerBrowserProps {
  toggle: (e: React.MouseEvent<HTMLElement>) => void;
  onWorkspaceCreate: () => void;
  enableRouting: boolean;
  scope: boolean | string;
  showPanelToggle: boolean;
}

export default class SpannerBrowser extends React.PureComponent<ISpannerBrowserProps> {
  public static defaultProps: Partial<ISpannerBrowserProps> = {
    enableRouting: true,
  };

  public render() {
    const instancesPath = '/ns/:namespace/connections/spanner/:connectionId';
    const databasesPath = `${instancesPath}/instances/:instanceId`;
    const tablesPath = `${databasesPath}/databases/:databaseId`;

    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="spanner-browser">
          {this.props.enableRouting ? (
            <DataPrepBrowserPageTitle
              browserI18NName="SpannerBrowser"
              browserStateName="spanner"
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
              <Route exact path={instancesPath} component={SpannerInstanceList} />
              <Route exact path={databasesPath} component={SpannerDatabaseList} />
              <Route
                exact
                path={tablesPath}
                render={(routeParams) => (
                  <SpannerTableList {...routeParams} scope={this.props.scope} />
                )}
              />
            </Switch>
          ) : (
            <SpannerDisplaySwitch
              onWorkspaceCreate={this.props.onWorkspaceCreate}
              scope={this.props.scope}
            />
          )}
        </div>
      </Provider>
    );
  }
}
