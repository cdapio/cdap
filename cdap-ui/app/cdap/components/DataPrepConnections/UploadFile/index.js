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
import FileDnD from 'components/FileDnD';
import NamespaceStore from 'services/NamespaceStore';
import cookie from 'react-cookie';
import isNil from 'lodash/isNil';
import UploadFile from 'services/upload-file';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';

require('./UploadFile.scss');

const PREFIX = 'features.DataPrepConnections.UploadComponent';
const FILE_SIZE_LIMIT = 10 * 1024 * 1024; // 10 MB

export default class ConnectionsUpload extends Component {
  constructor(props) {
    super(props);

    this.state = {
      file: '',
      recordDelimiter: '\\n',
      error: null
    };

    this.fileHandler = this.fileHandler.bind(this);
    this.recordDelimiterHandler = this.recordDelimiterHandler.bind(this);
    this.upload = this.upload.bind(this);
    this.clearError = this.clearError.bind(this);
  }

  fileHandler(e) {
    this.setState({
      file: e[0],
      error: e[0].size > FILE_SIZE_LIMIT
    });
  }

  recordDelimiterHandler(e) {
    this.setState({recordDelimiter: e.target.value});
  }

  upload() {
    if (!this.state.file) { return; }

    let delimiter = this.state.recordDelimiter;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let fileName = this.state.file.name;

    let url = `/namespaces/${namespace}/apps/dataprep/services/service/methods/workspaces`;

    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': fileName,
      'file': fileName
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
        try {
          let parsed = JSON.parse(res);
          let workspaceId = parsed.values[0].id;

          let navigatePath = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
          window.location.href = navigatePath;
        } catch (e) {
          console.log('error', e);
        }
      }, (err) => {
        console.log(err);
        this.setState({
          messageType: 'DANGER',
          message: err.message
        });
      });
  }

  clearError() {
    this.setState({error: false});
  }

  render() {
    let uploadDisabled = !this.state.file || this.state.file.size > FILE_SIZE_LIMIT;

    let error;
    if (this.state.error) {
      error = (
        <div className="upload-error">
          <span className="message">
            {T.translate(`${PREFIX}.fileSizeError`)}
          </span>

          <span
            className="close-button"
            onClick={this.clearError}
          >
            x
          </span>
        </div>
      );
    }

    return (
      <div className="connections-upload-container">
        {error}
        <div className="top-panel">
          <div className="title">
            <h5>
              <span
                className="fa fa-fw"
                onClick={this.props.toggle}
              >
                <IconSVG name="icon-bars" />
              </span>

              <span>
                {T.translate(`${PREFIX}.title`)}
              </span>
            </h5>
          </div>
        </div>

        <div className="upload-content-container">
          <div className="file-upload">
            <FileDnD
              onDropHandler={this.fileHandler}
              file={this.state.file}
            />
          </div>

          <div className="row upload-row">
            <div className="col-xs-6">
              <button
                className="btn btn-primary"
                onClick={this.upload}
                disabled={uploadDisabled}
              >
                {T.translate(`${PREFIX}.uploadButton`)}
              </button>

              <span className="helper-text">
                {T.translate(`${PREFIX}.helperText`)}
              </span>
            </div>

            <div className="col-xs-6 text-xs-right">
              <form className="form-inline">
                <div className="form-group">
                  <label className="control-label">
                    {T.translate(`${PREFIX}.recordDelimiter`)}
                  </label>

                  <input
                    type="text"
                    className="form-control"
                    onChange={this.recordDelimiterHandler}
                    value={this.state.recordDelimiter}
                  />
                </div>
              </form>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

ConnectionsUpload.propTypes = {
  toggle: PropTypes.func
};
