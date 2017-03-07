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
import WorkspaceModal from 'components/DataPrep/TopPanel/WorkspaceModal';
import DataPrepStore from 'components/DataPrep/store';
import ee from 'event-emitter';

require('./TopPanel.scss');

export default class DataPrepTopPanel extends Component {
  constructor(props) {
    super(props);

    let initialWorkspace = DataPrepStore.getState().dataprep.workspaceId;
    this.state = {
      workspaceId: initialWorkspace,
      workspaceModal: false,
    };

    this.toggleWorkspaceModal = this.toggleWorkspaceModal.bind(this);
    this.eventEmitter = ee(ee);

    this.eventEmitter.on('DATAPREP_NO_WORKSPACE_ID', this.toggleWorkspaceModal);

    this.sub = DataPrepStore.subscribe(() => {
      let storeWorkspace = DataPrepStore.getState().dataprep.workspaceId;
      this.setState({
        workspaceId: storeWorkspace
      });
    });
  }

  componentWillUnmount() {
    this.sub();
    this.eventEmitter.off('DATAPREP_NO_WORKSPACE_ID', this.toggleWorkspaceModal);
  }

  toggleWorkspaceModal() {
    this.setState({workspaceModal: !this.state.workspaceModal});
  }

  renderWorkspaceModal() {
    if (!this.state.workspaceModal) { return null; }

    return (
      <WorkspaceModal toggle={this.toggleWorkspaceModal} />
    );
  }

  render() {
    return (
      <div className="top-panel clearfix">
        <div className="left-title float-xs-left">
          <div className="data-prep-name">
            <strong>Data Preparation</strong>
            <span className="tag tag-success">BETA</span>
          </div>

          <div
            className="workspace-mgmt"
            onClick={this.toggleWorkspaceModal}
          >
            <span>
              {this.state.workspaceId}
              {this.renderWorkspaceModal()}
            </span>
            <span className="fa fa-pencil" />
          </div>
        </div>

        <div className="action-buttons float-xs-right">
          <button
            className="btn btn-primary"
            disabled
          >
            Add to Pipeline
          </button>

          <button
            className="btn btn-secondary"
            disabled
          >
            View Schema
          </button>
        </div>
      </div>
    );
  }
}
