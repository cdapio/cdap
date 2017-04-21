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

export default class WorkspacePropertiesModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      file: null,
      message: null,
      messageType: null,
      recordDelimiter: '\\n'
    };

    this.deleteWorkspace = this.deleteWorkspace.bind(this);
    this.fileChange = this.fileChange.bind(this);
    this.upload = this.upload.bind(this);
    this.handleRecordDelimiterChange = this.handleRecordDelimiterChange.bind(this);

  }

  deleteWorkspace() {
    let workspaceId = this.props.workspace;
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.delete({
      namespace,
      workspaceId
    }).subscribe((res) => {
      console.log(res);

      DataPrepStore.dispatch({
        type: DataPrepActions.setWorkspace,
        payload: {
          workspaceId: ''
        }
      });

      cookie.remove('DATAPREP_WORKSPACE', { path: '/' });

      this.props.onDelete();
      this.props.toggle();

    }, (err) => {
      console.log(err);

      this.setState({
        messageType: 'DANGER',
        message: err.message
      });
    });
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

    let url = `/namespaces/${namespace}/apps/dataprep/services/service/methods/workspaces/${this.props.workspace}/upload`;

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
              message: err.message || `${this.props.workspace} workspace does not exist`
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

  renderUploadSection() {
    return (
      <div>
        <div>
          <h5>Upload Data</h5>
          <h6>Select the file to be uploaded to the workspace</h6>
          <div className="file-input">
            <input
              type="file"
              className="form-control"
              onChange={this.fileChange}
            />
          </div>

          <div className="record-delimiter form-inline">
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
            Workspace "{this.props.workspace}"
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          {this.renderUploadSection()}

          <hr />

          <div className="clearfix">
            <div className="button-container float-xs-left">
              <button
                className="btn btn-primary"
                onClick={this.upload}
                disabled={!this.state.file}
              >
                Upload
              </button>
            </div>

            {
              this.props.singleWorkspaceMode ?
                null
              :
                <div className="button-container float-xs-right">
                  <button
                    className="btn btn-danger"
                    onClick={this.deleteWorkspace}
                  >
                    Delete this workspace
                  </button>
                </div>
            }
          </div>
        </ModalBody>

        {this.renderFooter()}
      </Modal>
    );
  }
}

WorkspacePropertiesModal.propTypes = {
  toggle: PropTypes.func.isRequired,
  workspace: PropTypes.string.isRequired,
  onDelete: PropTypes.func,
  singleWorkspaceMode: PropTypes.bool
};
