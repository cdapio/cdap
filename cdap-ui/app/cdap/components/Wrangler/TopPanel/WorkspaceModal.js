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
import MyWranglerApi from 'api/wrangler';
import UploadFile from 'services/upload-file';
import WranglerStore from 'components/Wrangler/store';
import WranglerActions from 'components/Wrangler/store/WranglerActions';
import CardActionFeedback from 'components/CardActionFeedback';
import cookie from 'react-cookie';

export default class WorkspaceModal extends Component {
  constructor(props) {
    super(props);

    let initialWorkspace = WranglerStore.getState().wrangler.workspaceId;

    this.state = {
      activeWorkspace: initialWorkspace,
      workspaceId: null,
      file: null,
      message: null,
      messageType: null
    };

    this.handleWorkspaceInput = this.handleWorkspaceInput.bind(this);
    this.setWorkspace = this.setWorkspace.bind(this);
    this.createWorkspace = this.createWorkspace.bind(this);
    this.deleteWorkspace = this.deleteWorkspace.bind(this);
    this.fileChange = this.fileChange.bind(this);
    this.upload = this.upload.bind(this);
    this.attemptModalClose = this.attemptModalClose.bind(this);

    this.sub = WranglerStore.subscribe(() => {
      this.setState({
        activeWorkspace: WranglerStore.getState().wrangler.workspaceId
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  handleWorkspaceInput(e) {
    this.setState({workspaceId: e.target.value});
  }

  setWorkspace() {
    if (!this.state.workspaceId) { return; }
    let workspaceId = this.state.workspaceId;

    let params = {
      namespace: 'default',
      workspaceId: workspaceId,
      limit: 100
    };

    MyWranglerApi.execute(params)
      .subscribe((res) => {
        cookie.save('WRANGLER_WORKSPACE', workspaceId);

        WranglerStore.dispatch({
          type: WranglerActions.setWorkspace,
          payload: {
            workspaceId,
            data: res.value,
            headers: res.header
          }
        });
      }, (err) => {
        console.log('err', err);
        this.setState({
          messageType: 'DANGER',
          message: err.message || `${workspaceId} workspace does not exist`
        });
      });
  }

  createWorkspace() {
    if (!this.state.workspaceId) { return; }
    let workspaceId = this.state.workspaceId;

    MyWranglerApi.create({
      namespace: 'default',
      workspaceId
    }).subscribe((res) => {
      this.setState({
        messageType: 'SUCCESS',
        message: res.message
      });

      cookie.save('WRANGLER_WORKSPACE', workspaceId);

      WranglerStore.dispatch({
        type: WranglerActions.setWorkspace,
        payload: {
          workspaceId
        }
      });

    }, (err) => {
      this.setState({
        messageType: 'DANGER',
        message: err.message
      });
    });
  }

  deleteWorkspace() {
    if (!this.state.workspaceId) { return; }
    let workspaceId = this.state.workspaceId;

    MyWranglerApi.delete({
      namespace: 'default',
      workspaceId
    }).subscribe((res) => {
      console.log(res);
      this.setState({
        messageType: 'SUCCESS',
        message: res.message
      });

      if (this.state.workspaceId === this.state.activeWorkspace) {
        WranglerStore.dispatch({
          type: WranglerActions.setWorkspace,
          payload: {
            workspaceId: ''
          }
        });

        cookie.remove('WRANGLER_WORKSPACE');
      }
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

  upload() {
    if (!this.state.file) { return; }

    let url = `/namespaces/default/apps/wrangler/services/service/methods/workspaces/${this.state.activeWorkspace}/upload`;
    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': name,
    };
    UploadFile({url, fileContents: this.state.file, headers})
      .subscribe((res) => {
        console.log(res);

        let params = {
          namespace: 'default',
          workspaceId: this.state.activeWorkspace,
          limit: 100
        };

        MyWranglerApi.execute(params)
          .subscribe((response) => {
            console.log('res', response);

            WranglerStore.dispatch({
              type: WranglerActions.setWorkspace,
              payload: {
                workspaceId: this.state.activeWorkspace,
                data: response.value,
                headers: response.header
              }
            });

            this.setState({
              messageType: 'SUCCESS',
              message: `Success uploading file to workspace ${this.state.activeWorkspace}`
            });
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

  renderUploadSection() {
    if (!this.state.activeWorkspace) { return null; }

    return (
      <div>
        <hr/>

        <div>
          <h5>Upload Data</h5>
          <h6>Select the file to be uploaded to active workspace</h6>
          <div className="file-input">
            <input
              type="file"
              className="form-control"
              onChange={this.fileChange}
            />
          </div>

          <div className="button-container">
            <button
              className="btn btn-secondary"
              onClick={this.upload}
              disabled={!this.state.file}
            >
              Upload
            </button>
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
    if (!this.state.activeWorkspace) { return; }

    this.props.toggle();
  }

  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.attemptModalClose}
        zIndex="1070"
        className="workspace-management-modal"
      >
        <ModalHeader>
          <span>
            Workspace
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          {
            this.state.activeWorkspace ? (
              <div>
                <h5>
                  Current Active Workspace: <em>{this.state.activeWorkspace}</em>
                </h5>
                <hr/>
              </div>
            ) : null
          }

          <div>
            <h5>Set or Create New Workspace</h5>
            <small>Create workspace before starting to wrangle</small>
            <input
              type="text"
              className="form-control"
              value={this.state.workspaceId}
              onChange={this.handleWorkspaceInput}
            />
          </div>
          <div className="button-container">
            <button
              className="btn btn-secondary"
              onClick={this.setWorkspace}
            >
              Set
            </button>
            <button
              className="btn btn-primary"
              onClick={this.createWorkspace}
            >
              Create
            </button>

            <button
              className="btn btn-danger float-xs-right"
              onClick={this.deleteWorkspace}
            >
              Delete
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
  toggle: PropTypes.func.isRequired
};
