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
import {objectQuery} from 'services/helpers';
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import T from 'i18n-react';
import isNil from 'lodash/isNil';
import CreateDatasetBtn from 'components/DataPrep/TopPanel/CreateDatasetBtn';
import classnames from 'classnames';

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
      onSubmitLoading: false,
      workspaceInfo: initialState.workspaceInfo
    };

    this.toggleSchemaModal = this.toggleSchemaModal.bind(this);
    this.toggleAddToPipelineModal = this.toggleAddToPipelineModal.bind(this);
    this.toggleUpgradeModal = this.toggleUpgradeModal.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;
      this.setState({
        higherVersion: state.higherVersion,
        workspaceInfo: state.workspaceInfo
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

  renderTopPanelDisplay() {
    let info = this.state.workspaceInfo;
    if (info) {
      if (info.properties.connection === 'file') {
        return (
          <div className={classnames("data-prep-name", {"upgrade": this.state.higherVersion})}>
            <div className="connection-type">
              {T.translate('features.DataPrep.TopPanel.file')}
            </div>
            <div className="title">
              {info.properties.file}
            </div>
          </div>
        );
      } else if (info.properties.connection === 'database') {
        return (
          <div className={classnames("data-prep-name", {"upgrade": this.state.higherVersion})}>
            <div className="connection-type">
              {T.translate('features.DataPrep.TopPanel.database')}
            </div>
            <div className="title">
              SELECT * FROM {info.properties.name}
            </div>
          </div>
        );
      }
    }

    return (
      <div className="data-prep-name">
        <strong>
          {T.translate('features.DataPrep.TopPanel.title')}
        </strong>
        <span className="tag tag-success">BETA</span>
      </div>
    );
  }

  render() {
    return (
      <div className="row top-panel clearfix">
        <div className="left-title">
          <div className="upper-section">
            {this.renderTopPanelDisplay()}

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
        </div>

        <div className="action-buttons">
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
              disabled={this.state.onSubmitLoading || isNil(this.state.workspaceInfo) ? 'disabled' : false}
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

          <div className="secondary-actions">
            {
              this.props.singleWorkspaceMode ?
                null
              :
                <CreateDatasetBtn
                  disabledState={isNil(this.state.workspaceInfo) || !objectQuery(this.state, 'workspaceInfo', 'properties', 'path')}
                  title={!objectQuery(this.state, 'workspaceInfo', 'properties', 'path') ? T.translate('features.DataPrep.TopPanel.copyToCDAPDatasetBtn.uploadDisabledMessage') : null}
                />
            }
            <button
              className="btn btn-link"
              onClick={this.toggleSchemaModal}
              disabled={isNil(this.state.workspaceInfo) ? 'disabled' : false}
            >
              {T.translate('features.DataPrep.TopPanel.viewSchemaBtnLabel')}
            </button>
          </div>
          {this.renderSchemaModal()}
        </div>
      </div>
    );
  }
}

DataPrepTopPanel.propTypes = {
  singleWorkspaceMode: PropTypes.bool,
  onSubmit: PropTypes.func
};
