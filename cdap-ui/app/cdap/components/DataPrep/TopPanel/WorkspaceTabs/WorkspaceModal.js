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
import {Modal, ModalHeader, ModalBody} from 'reactstrap';
import MyDataPrepApi from 'api/dataprep';
import UploadFile from 'services/upload-file';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import CardActionFeedback from 'components/CardActionFeedback';
import cookie from 'react-cookie';
import isNil from 'lodash/isNil';
import NamespaceStore from 'services/NamespaceStore';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import T from 'i18n-react';

export default class WorkspaceModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      activeWorkspace: DataPrepStore.getState().dataprep.workspaceId,
      workspaceId: null,
      file: null,
      message: null,
      messageType: null,
      recordDelimiter: '\\n',
      workspaceCreated: false
    };

    this.handleWorkspaceInput = this.handleWorkspaceInput.bind(this);
    this.createWorkspace = this.createWorkspace.bind(this);
    this.fileChange = this.fileChange.bind(this);
    this.upload = this.upload.bind(this);
    this.attemptModalClose = this.attemptModalClose.bind(this);
    this.handleRecordDelimiterChange = this.handleRecordDelimiterChange.bind(this);
    this.createAndUpload = this.createAndUpload.bind(this);
    this.sub = DataPrepStore.subscribe(() => {
      this.setState({
        activeWorkspace: DataPrepStore.getState().dataprep.workspaceId
      });
    });
  }

  componentDidMount() {
    this.workspaceInputRef.focus();
  }

  componentWillUnmount() {
    this.sub();
  }

  handleWorkspaceInput(e) {
    this.setState({workspaceId: e.target.value});
  }

  createWorkspace() {
    if (!this.state.workspaceId) { return; }
    let workspaceId = this.state.workspaceId;
    let namespace = NamespaceStore.getState().selectedNamespace;

    let createObservable = MyDataPrepApi.create({
      namespace,
      workspaceId
    });
    createObservable.subscribe((res) => {
      this.setState({
        messageType: 'SUCCESS',
        message: res.message,
        workspaceCreated: true
      });

      cookie.save('DATAPREP_WORKSPACE', workspaceId, { path: '/' });

      DataPrepStore.dispatch({
        type: DataPrepActions.setWorkspace,
        payload: {
          workspaceId
        }
      });

      this.props.onCreate();

    }, (err) => {
      this.setState({
        messageType: 'DANGER',
        message: err.message || err.data || err
      });
    });
    return createObservable;
  }

  fileChange(e) {
    this.setState({file: e.target.files[0]});
  }

  handleRecordDelimiterChange(e) {
    this.setState({recordDelimiter: e.target.value});
  }

  upload() {
    if (!this.state.file) { return; }

    let delimiter = this.state.recordDelimiter;
    let namespace = NamespaceStore.getState().selectedNamespace;

    let url = `/namespaces/${namespace}/apps/dataprep/services/service/methods/workspaces/${this.state.activeWorkspace}/upload`;

    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': name
    };

    if (window.CDAP_CONFIG.securityEnabled) {
      let token = cookie.load('CDAP_Auth_Token');
      if (!isNil(token)) {
        headers.Authorization = `Bearer ${token}`;
      }
    }

    if (delimiter) {
      headers['recorddelimiter'] = delimiter;
    }

    UploadFile({url, fileContents: this.state.file, headers})
      .subscribe((res) => {
        console.log(res);

        execute([], true)
          .subscribe(() => {
            this.props.toggle();
          }, (err) => {
            console.log('err', err);
            this.setState({
              messageType: 'DANGER',
              message: err.message || `${this.state.activeWorkspace} workspace does not exist`
            });
          });

      }, (err) => {
        console.log(err);
        this.setState({
          messageType: 'DANGER',
          message: err.message
        });
      });
  }

  createAndUpload() {
    this.createWorkspace()
        .subscribe(() => {
          this.upload();
        });
  }
  renderUploadSection() {
    if (!this.state.activeWorkspace) { return null; }

    return (
      <div>
        <hr />

        <div>
          <h5>{T.translate('features.DataPrep.TopPanel.WorkspaceModal.uploadTitle')}</h5>
          <h6>{T.translate('features.DataPrep.TopPanel.WorkspaceModal.uploadSubTitle')}</h6>
          <div className="file-input">
            <input
              type="file"
              className="form-control"
              onChange={this.fileChange}
            />
          </div>

          <div className="clearfix">
            <div className="button-container float-xs-left">
              <button
                className="btn btn-primary"
                onClick={this.createAndUpload}
                disabled={!this.state.file}
              >
                {
                  this.state.workspaceCreated ?
                    T.translate('features.DataPrep.TopPanel.WorkspaceModal.uploadBtnLabel')
                  :
                    T.translate('features.DataPrep.TopPanel.WorkspaceModal.createAndUploadBtnLabel')}
              </button>
            </div>

            <div className="record-delimiter form-inline float-xs-right">
              <label className="label-control">Record Delimiter:</label>
              <input
                type="text"
                className="form-control"
                value={this.state.recordDelimiter}
                onChange={this.handleRecordDelimiterChange}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }

  renderFooter() {
    if (!this.state.message) { return null; }

    return (
      <CardActionFeedback
        type={this.state.messageType}
        message={this.state.message}
      />
    );
  }

  attemptModalClose() {
    if (!this.state.activeWorkspace || this.props.isEmpty) { return; }

    this.props.toggle();
  }

  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.attemptModalClose}
        className="workspace-management-modal"
        zIndex="1061"
      >
        <ModalHeader>
          <span>
            {T.translate('features.DataPrep.TopPanel.WorkspaceModal.createModalTitle')}
          </span>

          {
            this.props.isEmpty ? null : (
              <div
                className="close-section float-xs-right"
                onClick={this.attemptModalClose}
              >
                <span className="fa fa-times" />
              </div>
            )
          }
        </ModalHeader>
        <ModalBody>
          <div>
            <h5>{T.translate('features.DataPrep.TopPanel.WorkspaceModal.createTitle')}</h5>
            <input
              type="text"
              className="form-control"
              value={this.state.workspaceId}
              onChange={this.handleWorkspaceInput}
              ref={ref => this.workspaceInputRef = ref}
            />
          </div>
          <div className="button-container">
            <button
              className="btn btn-primary"
              onClick={this.createWorkspace}
            >
              {T.translate('features.DataPrep.TopPanel.WorkspaceModal.create')}
            </button>
          </div>

          {this.renderUploadSection()}

        </ModalBody>

        {this.renderFooter()}
      </Modal>
    );
  }
}

WorkspaceModal.propTypes = {
  toggle: PropTypes.func.isRequired,
  onCreate: PropTypes.func,
  isEmpty: PropTypes.bool
};
