/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import MyWranglerApi from 'api/wrangler';
import UploadFile from 'services/upload-file';
import shortid from 'shortid';
import SchemaStore from 'components/SchemaEditor/SchemaStore';
import SchemaEditor from 'components/SchemaEditor';
import {getParsedSchema} from 'components/SchemaEditor/SchemaHelpers';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';

require('./Experimental.scss');

export default class Experimental extends Component {
  constructor(props) {
    super(props);

    this.state = {
      file: null,
      data: null,
      header: null,
      length: 0,
      schemaModal: false,
      showAlert: false,
      errorMessage: '',
      successMessage: '',
      successAlert: false,
      validation: null,
      statistics: null
    };

    this.createWorkspace = this.createWorkspace.bind(this);
    this.deleteWorkspace = this.deleteWorkspace.bind(this);
    this.fileChange = this.fileChange.bind(this);
    this.upload = this.upload.bind(this);
    this.execute = this.execute.bind(this);
    this.getSummary = this.getSummary.bind(this);
    this.getSchema = this.getSchema.bind(this);
    this.toggleSchemaModal = this.toggleSchemaModal.bind(this);
    this.renderSummary = this.renderSummary.bind(this);
    this.dismissAlert = this.dismissAlert.bind(this);
    this.dismissSuccess = this.dismissSuccess.bind(this);
  }

  createWorkspace() {
    let workspaceId = this.workspaceId.value;
    if (!workspaceId) {
      this.setState({
        errorMessage: 'Please fill out workspace id',
        showAlert: true
      });

      return;
    }

    MyWranglerApi.create({
      namespace: 'default',
      workspaceId
    }).subscribe((res) => {
      console.log(res);
      this.showSuccess(res.message);

    }, (err) => {
      console.log(err);
      this.setState({
        errorMessage: err.message,
        showAlert: true
      });
    });
  }

  showSuccess(message) {
    this.setState({
      successAlert: true,
      successMessage: message
    });

    setTimeout(() => {
      this.setState({successAlert: false});
    }, 3000);
  }

  deleteWorkspace() {
    let workspaceId = this.workspaceId.value;
    if (!workspaceId) {
      this.setState({
        errorMessage: 'Please fill out workspace id',
        showAlert: true
      });
      return;
    }

    MyWranglerApi.delete({
      namespace: 'default',
      workspaceId
    }).subscribe((res) => {
      console.log(res);
      this.showSuccess(res.message);
    }, (err) => {
      console.log(err);
      this.setState({
        errorMessage: err.message,
        showAlert: true
      });
    });
  }

  fileChange(e) {
    this.setState({file: e.target.files[0]});
  }

  upload() {
    if (!this.workspaceId.value) {
      this.setState({
        errorMessage: 'Please fill out workspace id',
        showAlert: true
      });

      return;
    }

    if (!this.state.file) {
      this.setState({
        errorMessage: 'Please select a file',
        showAlert: true
      });
      return;
    }

    let url = `/namespaces/default/apps/wrangler/services/service/methods/workspaces/${this.workspaceId.value}/upload`;
    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': name,
    };
    UploadFile({url, fileContents: this.state.file, headers})
      .subscribe((res) => {
        console.log(res);
        this.showSuccess(`Success uploading file to workspace ${this.workspaceId.value}`);
        this.execute();
      }, (err) => {
        console.log(err);
        this.setState({
          errorMessage: err.message,
          showAlert: true
        });
      });
  }

  execute() {
    let workspaceId = this.workspaceId.value;
    if (!workspaceId) {
      this.setState({
        errorMessage: 'Please specify the workspace you want to use.',
        showAlert: true
      });
      return;
    }

    let requestObj = {
      namespace: 'default',
      workspaceId,
      limit: 100
    };

    let directives = this.textareaRef.value;

    if (directives) {
      directives = directives.split('\n');
      requestObj.directive = directives;
    }

    MyWranglerApi.execute(requestObj)
    .subscribe((res) => {
      this.setState({
        data: res.value,
        header: res.header,
        length: res.item
      });
    }, (err) => {
      this.setState({
        errorMessage: err.message || err.response.message,
        showAlert: true
      });
    });
  }

    getSummary() {
    let workspaceId = this.workspaceId.value;
    if (!workspaceId) {
      this.setState({
        errorMessage: 'Please specify the workspace you want to use.',
        showAlert: true
      });
      return;
    }

    let requestObj = {
      namespace: 'default',
      workspaceId,
      limit: 100
    };

    let directives = this.textareaRef.value;

    if (directives) {
      directives = directives.split('\n');
      requestObj.directive = directives;
    }

    MyWranglerApi.summary(requestObj)
    .subscribe((res) => {
      console.log(res);
      this.setState({
        validation: res.value.validation,
        statistics: res.value.statistics
      });
      this.renderSummary();
    }, (err) => {
      this.setState({
        errorMessage: err.message || err.response.message,
        showAlert: true
      });
    });
  }

  getSchema() {
    let workspaceId = this.workspaceId.value;
    if (!workspaceId) {
      this.setState({
        errorMessage: 'Please fill out workspace id',
        showAlert: true
      });
      return;
    }

    let requestObj = {
      namespace: 'default',
      workspaceId
    };

    let directives = this.textareaRef.value;

    if (directives) {
      directives = directives.split('\n');
      requestObj.directive = directives;
    }

    MyWranglerApi.getSchema(requestObj)
    .subscribe((res) => {
      let tempSchema = {
        name: 'avroSchema',
        type: 'record',
        fields: res
      };
      let fields = getParsedSchema(tempSchema);
      SchemaStore.dispatch({
        type: 'FIELD_UPDATE',
        payload: {
          schema: { fields }
        }
      });

      this.toggleSchemaModal();
    }, (err) => {
      this.setState({
        errorMessage: err.message || err.response.message,
        showAlert: true
      });
    });
  }

  renderSummary() {
    if (!this.state.validation) { return null; }
    if (!this.state.statistics) { return null; }

    return (
      <Modal
        isOpen={this.state.validation}
        toggle={() => this.setState({validation: null})}
      >
      <ModalHeader>
        <h2>Summary</h2>
      </ModalHeader>
         <ModalBody>
           <pre>{JSON.stringify(this.state.validation, null, 2)}</pre>
           <pre>{JSON.stringify(this.state.statistics, null, 2)}</pre>
         </ModalBody>
      </Modal>
    );
  }

  toggleSchemaModal() {
    this.setState({
      schemaModal: !this.state.schemaModal
    });
  }

  renderModal() {
    if (!this.state.schemaModal) { return null; }

    return (
      <Modal
        isOpen={this.state.schemaModal}
        toggle={this.toggleSchemaModal}
        zIndex="1070"
        className="experimental-schema-modal"
      >
        <ModalHeader>
          <span>
            Schema
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.toggleSchemaModal}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <fieldset disabled>
            <SchemaEditor />
          </fieldset>
        </ModalBody>
      </Modal>
    );
  }

  dismissAlert() {
    this.setState({showAlert: false});
  }

  dismissSuccess() {
    this.setState({successAlert: false});
  }

  renderAlert() {
    if (!this.state.showAlert) { return null; }

    return (
      <div className="alert alert-danger" role="alert">
        <span>{this.state.errorMessage}</span>
        <span
          className="float-xs-right"
          onClick={this.dismissAlert}
        >
          <i className="fa fa-times"></i>
        </span>
      </div>
    );
  }

  renderSuccessAlert() {
    if (!this.state.successAlert) { return null; }

    return (
      <div className="alert alert-success" role="alert">
        <span>{this.state.successMessage}</span>
        <span
          className="float-xs-right"
          onClick={this.dismissSuccess}
        >
          <i className="fa fa-times"></i>
        </span>
      </div>
    );
  }

  render() {
    let headers;
    let displayheaders;
    if (this.state.length !== 0) {
      headers = this.state.header;
      displayheaders = headers.map((header) => {
         return header;
      });
    }

    return (
      <div className="experimental-container">
        {this.renderSuccessAlert()}
        {this.renderAlert()}
        <div className="row">
          <div className="col-xs-9 left-panel">
            {this.state.length === 0 ?
              (<h3 className="text-xs-center no-data">Create workspace and upload data.</h3>)
                : (
              <table className="table table-bordered table-striped">
                <thead className="thead-inverse">
                  {
                    displayheaders.map((head) => {
                      return <th key={head}>{head}</th>;
                    })
                  }
                </thead>
                <tbody>
                  {
                    this.state.data.map((row) => {
                      return (
                        <tr key={shortid.generate()}>
                          {headers.map((head) => {
                            return <td key={shortid.generate()}><div>{row[head]}</div></td>;
                          })}
                        </tr>
                      );
                    })
                  }
                </tbody>
              </table>
            )}
          </div>
          <div className="col-xs-3 right-panel">
            <div className="workspace-container">
              <h4>Workspace</h4>
              <h6>Create workspace before starting to wrangle</h6>
              <div>
                <input
                  type="text"
                  className="form-control"
                  ref={(ref) => {this.workspaceId = ref;}}
                />
              </div>

              <div className="text-xs-right">
                <button
                  className="btn btn-danger"
                  onClick={this.deleteWorkspace}
                >
                  Delete
                </button>

                <button
                  className="btn btn-secondary"
                  onClick={this.createWorkspace}
                >
                  Create
                </button>
              </div>

              <h6>Select the file to be uploaded to workspace </h6>
              <div className="file-input">
                <input
                  type="file"
                  className="form-control"
                  onChange={this.fileChange}
                />
              </div>

              <div className="text-xs-right">
                <button
                  className="btn btn-secondary"
                  onClick={this.upload}
                >
                  Upload
                </button>
              </div>

            </div>
            <div className="directives-container">
              <div>
                <h4>Directives</h4>
                <h6>Specify directive to wrangle data. For more information visit <a href="http://github.com/hydrator/wrangler-transform">here</a></h6>
                <textarea
                  className="form-control"
                  ref={(ref) => { this.textareaRef = ref; }}
                ></textarea>
              </div>

              <div className="text-xs-right">
                <button
                  className="btn btn-secondary"
                  onClick={this.getSummary}
                >
                  Summary
                </button>
                <button
                  className="btn btn-secondary"
                  onClick={this.getSchema}
                >
                  Schema
                </button>

                <button
                  className="btn btn-primary"
                  onClick={this.execute}
                >
                  Apply
                </button>
              </div>
            </div>
          </div>
        </div>

        {this.renderModal()}
        {this.renderSummary()}
      </div>
    );
  }
}
