/*
 * Copyright © 2017 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
import DataPrepStore from 'components/DataPrep/store';
import classnames from 'classnames';
import ColumnsTab from 'components/DataPrep/DataPrepSidePanel/ColumnsTab';
import TargetTab from 'components/DataPrep/DataPrepSidePanel/TargetTab';
import DirectivesTab from 'components/DataPrep/DataPrepSidePanel/DirectivesTab';
import T from 'i18n-react';
import If from 'components/If';

require('./DataPrepSidePanel.scss');
const PREFIX = 'features.DataPrep.DataPrepSidePanel';

export default class DataPrepSidePanel extends Component {
  constructor(props) {
    super(props);

    let storeState = DataPrepStore.getState().dataprep;

    this.state = {
      activeTab: 1,
      deleteHover: null,
      headers: storeState.headers,
      directives: storeState.directives,
      summary: {},
    };
  }

  componentDidMount() {
    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;

      this.setState({
        headers: state.headers,
        directives: state.directives,
      });
    });
  }

  componentWillUnmount() {
    if (this.sub && typeof this.sub === 'function') {
      this.sub();
    }
  }

  setActiveTab(tab) {
    this.setState({ activeTab: tab });
  }

  renderColumns() {
    if (this.state.headers.length === 0) {
      return <h5 className="empty-message text-center">{T.translate(`${PREFIX}.noColumns`)}</h5>;
    }

    return (
      <div className="tab-content">
        <ColumnsTab />
      </div>
    );
  }

  renderDirectives() {
    if (this.state.directives.length === 0) {
      return <h5 className="empty-message text-center">{T.translate(`${PREFIX}.noDirectives`)}</h5>;
    }

    return (
      <div className="tab-content">
        <DirectivesTab />
      </div>
    );
  }

  renderTarget() {
    return (
      <If condition={Theme.showWranglerDatamodelViewer}>
        <div className="tab-content">
          <TargetTab />
        </div>
      </If>
    );
  }

  renderTabContent() {
    switch (this.state.activeTab) {
      case 1:
        return this.renderColumns();
      case 2:
        return this.renderDirectives();
      case 3:
        return this.renderTarget();

      default:
        return null;
    }
  }

  render() {
    return (
      <div className="col-3 dataprep-side-panel" data-cy="dataprep-side-panel">
        <div className="tabs">
          <div className="tabs-headers">
            <div
              className={classnames('tab', { active: this.state.activeTab === 1 })}
              onClick={this.setActiveTab.bind(this, 1)}
            >
              {T.translate(`${PREFIX}.columnsTabLabel`, {
                columnsCount: this.state.headers.length,
              })}
            </div>
            <div
              className={classnames('tab', { active: this.state.activeTab === 2 })}
              onClick={this.setActiveTab.bind(this, 2)}
            >
              {T.translate(`${PREFIX}.directivesTabLabel`, {
                directivesCount: this.state.directives.length,
              })}
            </div>
            <If condition={Theme.showWranglerDatamodelViewer}>
              <div
                className={classnames('tab', { active: this.state.activeTab === 3 })}
                onClick={this.setActiveTab.bind(this, 3)}
              >
                {T.translate(`${PREFIX}.targetTabLabel`)}
              </div>
            </If>
          </div>

          {this.renderTabContent()}
        </div>
      </div>
    );
  }
}
