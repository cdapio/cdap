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
import DataPrepStore from 'components/DataPrep/store';
import classnames from 'classnames';
import ColumnsTab from 'components/DataPrep/DataPrepSidePanel/ColumnsTab';
import DirectivesTab from 'components/DataPrep/DataPrepSidePanel/DirectivesTab';

require('./DataPrepSidePanel.scss');

export default class DataPrepSidePanel extends Component {
  constructor(props) {
    super(props);

    let storeState = DataPrepStore.getState().dataprep;

    this.state = {
      activeTab: 1,
      deleteHover: null,
      headers: storeState.headers,
      directives: storeState.directives,
      summary: {}
    };

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;

      this.setState({
        headers: state.headers,
        directives: state.directives
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  setActiveTab(tab) {
    this.setState({activeTab: tab});
  }

  renderColumns() {
    if (this.state.headers.length === 0) {
      return (
        <h5 className="empty-message text-xs-center">
          No Columns
        </h5>
      );
    }

    return (
      <div className="tab-content">
        <ColumnsTab />
      </div>
    );
  }

  renderDirectives() {
    if (this.state.directives.length === 0) {
      return (
        <h5 className="empty-message text-xs-center">
          No Directives
        </h5>
      );
    }

    return (
      <div className="tab-content">
        <DirectivesTab />
      </div>
    );
  }

  renderTabContent() {
    switch (this.state.activeTab) {
      case 1:
        return this.renderDirectives();
      case 2:
        return this.renderColumns();
      default:
        return null;
    }
  }

  render() {
    return (
      <div className="col-xs-3 dataprep-side-panel">
        <div className="tabs">
          <div className="tabs-headers">
            <div
              className={classnames('tab', { 'active': this.state.activeTab === 1 })}
              onClick={this.setActiveTab.bind(this, 1)}
            >
              Directives ({this.state.directives.length})
            </div>
            <div
              className={classnames('tab', { 'active': this.state.activeTab === 2 })}
              onClick={this.setActiveTab.bind(this, 2)}
            >
              Columns ({this.state.headers.length})
            </div>
          </div>

          {this.renderTabContent()}
        </div>
      </div>
    );
  }
}
