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

import React, { Component, PropTypes } from 'react';
import DataPrepStore from 'components/DataPrep/store';
import SchemaModal from 'components/DataPrep/TopPanel/SchemaModal';
import AddToPipelineModal from 'components/DataPrep/TopPanel/AddToPipelineModal';
import UpgradeModal from 'components/DataPrep/TopPanel/UpgradeModal';
import WorkspaceTabs from 'components/DataPrep/TopPanel/WorkspaceTabs';
import {objectQuery} from 'services/helpers';
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import T from 'i18n-react';

require('./TopPanel.scss');

export default class DataPrepTopPanel extends Component {
  constructor(props) {
    super(props);

    let initialState = DataPrepStore.getState().dataprep;
    this.state = {
      workspaceModal: false,
      schemaModal: false,
      addToPipelineModal: false,
      upgradeModal: false,
      higherVersion: initialState.higherVersion,
      onSubmitError: null,
      onSubmitLoading: false
    };

    this.toggleSchemaModal = this.toggleSchemaModal.bind(this);
    this.toggleAddToPipelineModal = this.toggleAddToPipelineModal.bind(this);
    this.toggleUpgradeModal = this.toggleUpgradeModal.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;
      this.setState({
        higherVersion: state.higherVersion
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  toggleSchemaModal() {
    this.setState({schemaModal: !this.state.schemaModal});
  }

  toggleAddToPipelineModal() {
    this.setState({addToPipelineModal: !this.state.addToPipelineModal});
  }

  toggleUpgradeModal() {
    this.setState({upgradeModal: !this.state.upgradeModal});
  }

  renderSchemaModal() {
    if (!this.state.schemaModal) { return null; }

    return (
      <SchemaModal toggle={this.toggleSchemaModal} />
    );
  }

  renderAddToPipelineModal() {
    if (!this.state.addToPipelineModal) { return null; }

    return (
      <AddToPipelineModal toggle={this.toggleAddToPipelineModal} />
    );
  }

  renderUpgradeModal() {
    if (!this.state.upgradeModal) { return null; }

    return (
      <UpgradeModal toggle={this.toggleUpgradeModal} />
    );
  }

  onSubmit() {
    if (this.props.onSubmit) {
      let directives = DataPrepStore.getState().dataprep.directives;
      let workspaceId = DataPrepStore.getState().dataprep.workspaceId;
      let namespace = NamespaceStore.getState().selectedNamespace;
      let requestObj = {
        namespace,
        workspaceId
      };
      let requestBody = directiveRequestBodyCreator(directives);
      this.setState({
        onSubmitLoading: true,
        onSubmitError: null
      });
      MyDataPrepApi
        .getSchema(requestObj, requestBody)
        .subscribe(
          res => {
            let schema = {
              name: 'avroSchema',
              type: 'record',
              fields: res
            };
            try {
              getParsedSchemaForDataPrep(schema);
            } catch (e) {
              this.setState({
                onSubmitError: e.message,
                onSubmitLoading: false
              });
              return;
            }
            if (this.props.onSubmit) {
              this.props.onSubmit({
                workspaceId,
                directives,
                schema: JSON.stringify(schema)
              });
            }
          },
          (err) => {
            this.setState({
              onSubmitError: objectQuery(err, 'response', 'message') || JSON.stringify(err),
              onSubmitLoading: false
            });
          }
        );
    }
  }

  render() {
    return (
      <div className="top-panel clearfix">
        <div className="left-title float-xs-left">
          <div className="upper-section">
            <div className="data-prep-name">
              <strong>Data Preparation</strong>
              <span className="tag tag-success">BETA</span>
            </div>

            <div className="upgrade-button">
              {
                this.state.higherVersion ? (
                  <button
                    className="btn btn-info btn-sm"
                    onClick={this.toggleUpgradeModal}
                  >
                    <span className="fa fa-wrench fa-fw" />
                    {T.translate('features.DataPrep.TopPanel.upgradeBtnLabel')}
                  </button>
                ) : null
              }
              {this.renderUpgradeModal()}
            </div>
          </div>

          <WorkspaceTabs
            singleWorkspaceMode={this.props.singleWorkspaceMode}
            workspaceId={this.props.workspaceId}
          />
        </div>

        <div className="action-buttons float-xs-right">
          {
            this.state.onSubmitError ?
              <span className="text-danger">{this.state.onSubmitError}</span>
            :
              null
          }
          {
            !this.props.singleWorkspaceMode ?
              <button
                className="btn btn-primary"
                onClick={this.toggleAddToPipelineModal}
              >
                {T.translate('features.DataPrep.TopPanel.addToPipelineBtnLabel')}
              </button>
            :
            <button
              className="btn btn-primary"
              onClick={this.onSubmit.bind(this)}
              disabled={this.state.onSubmitLoading ? 'disabled' : null}
            >
              {
                this.state.onSubmitLoading ?
                  <span className="fa fa-spinner fa-spin"></span>
                :
                  null
              }
              <span>{T.translate('features.DataPrep.TopPanel.applyBtnLabel')}</span>
            </button>
          }
          {this.renderAddToPipelineModal()}

          <button
            className="btn btn-secondary"
            onClick={this.toggleSchemaModal}
          >
            {T.translate('features.DataPrep.TopPanel.viewSchemaBtnLabel')}
          </button>
          {this.renderSchemaModal()}
        </div>
      </div>
    );
  }
}

DataPrepTopPanel.propTypes = {
  singleWorkspaceMode: PropTypes.bool,
  workspaceId: PropTypes.string,
  onSubmit: PropTypes.func
};
