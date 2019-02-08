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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import CardActionFeedback, { CARD_ACTION_TYPES } from 'components/CardActionFeedback';
import { objectQuery } from 'services/helpers';
import ee from 'event-emitter';
import BtnWithLoading from 'components/BtnWithLoading';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';

const PREFIX = 'features.DataPrepConnections.AddConnections.GCS';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-3 col-form-label text-right';
const INPUT_COL_CLASS = 'col-8';

require('./GCSConnection.scss');

export default class GCSConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      error: null,
      name: '',
      projectId: '',
      serviceAccountKeyfile: '',
      testConnectionLoading: false,
      connectionResult: {
        message: null,
        type: null,
      },
    };

    this.eventEmitter = ee(ee);
    this.addConnection = this.addConnection.bind(this);
    this.editConnection = this.editConnection.bind(this);
    this.testConnection = this.testConnection.bind(this);
  }

  componentWillMount() {
    if (this.props.mode === 'ADD') {
      return;
    }

    this.setState({ loading: true });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      context: namespace,
      connectionId: this.props.connectionId,
    };

    MyDataPrepApi.getConnection(params).subscribe(
      (res) => {
        let info = objectQuery(res, 'values', 0),
          projectId = objectQuery(info, 'properties', 'projectId'),
          serviceAccountKeyfile = objectQuery(info, 'properties', 'service-account-keyfile');

        let name = this.props.mode === 'EDIT' ? info.name : '';

        this.setState({
          name,
          projectId,
          serviceAccountKeyfile,
          loading: false,
        });
      },
      (err) => {
        console.log('failed to fetch connection detail', err);

        this.setState({
          loading: false,
        });
      }
    );
  }

  constructProperties = () => {
    const properties = {};

    if (this.state.projectId && this.state.projectId.length > 0) {
      properties.projectId = this.state.projectId.trim();
    }

    if (this.state.serviceAccountKeyfile && this.state.serviceAccountKeyfile.length > 0) {
      properties['service-account-keyfile'] = this.state.serviceAccountKeyfile.trim();
    }

    return properties;
  };

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.GCS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.createConnection({ context: namespace }, requestBody).subscribe(
      () => {
        this.setState({ error: null });
        this.props.onAdd();
        this.props.close();
      },
      (err) => {
        console.log('err', err);

        let error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response');
        this.setState({ error });
      }
    );
  }

  editConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      context: namespace,
      connectionId: this.props.connectionId,
    };

    let requestBody = {
      name: this.state.name,
      id: this.props.connectionId,
      type: ConnectionType.GCS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.updateConnection(params, requestBody).subscribe(
      () => {
        this.setState({ error: null });
        this.eventEmitter.emit('DATAPREP_CONNECTION_EDIT_GCS', this.props.connectionId);
        this.props.onAdd();
        this.props.close();
      },
      (err) => {
        console.log('err', err);

        let error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response');
        this.setState({ error });
      }
    );
  }

  testConnection() {
    this.setState({
      testConnectionLoading: true,
      connectionResult: {
        message: null,
        type: null,
      },
      error: null,
    });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.GCS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.gcsTestConnection({ context: namespace }, requestBody).subscribe(
      (res) => {
        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.SUCCESS,
            message: res.message,
          },
          testConnectionLoading: false,
        });
      },
      (err) => {
        console.log('Error testing Google Cloud Storage connection', err);

        let errorMessage =
          objectQuery(err, 'response', 'message') ||
          objectQuery(err, 'response') ||
          T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.DANGER,
            message: errorMessage,
          },
          testConnectionLoading: false,
        });
      }
    );
  }

  handleChange(key, e) {
    this.setState({
      [key]: e.target.value,
    });
  }

  renderTestButton() {
    let disabled = !this.state.name || this.state.testConnectionLoading;

    return (
      <BtnWithLoading
        loading={this.state.testConnectionLoading}
        label={T.translate(`${PREFIX}.testConnection`)}
        className="btn btn-secondary"
        onClick={this.testConnection}
        disabled={disabled}
        darker={true}
      />
    );
  }

  renderAddConnectionButton() {
    let disabled = !this.state.name;

    let onClickFn = this.addConnection;

    if (this.props.mode === 'EDIT') {
      onClickFn = this.editConnection;
    }

    return (
      <ModalFooter>
        <button className="btn btn-primary" onClick={onClickFn} disabled={disabled}>
          {T.translate(`${PREFIX}.Buttons.${this.props.mode}`)}
        </button>

        {this.renderTestButton()}
      </ModalFooter>
    );
  }

  renderContent() {
    if (this.state.loading) {
      return (
        <div className="gcs-detail text-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="gcs-detail">
        <div className="form">
          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.name`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.name}
                  onChange={this.handleChange.bind(this, 'name')}
                  disabled={this.props.mode === 'EDIT'}
                  placeholder={T.translate(`${PREFIX}.Placeholders.name`)}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>{T.translate(`${PREFIX}.projectId`)}</label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.projectId}
                  onChange={this.handleChange.bind(this, 'projectId')}
                  placeholder={T.translate(`${PREFIX}.Placeholders.projectId`)}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.serviceAccountKeyfile`)}
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.serviceAccountKeyfile}
                  onChange={this.handleChange.bind(this, 'serviceAccountKeyfile')}
                  placeholder={T.translate(`${PREFIX}.Placeholders.serviceAccountKeyfile`)}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  renderError() {
    if (!this.state.error && !this.state.connectionResult.message) {
      return null;
    }

    if (this.state.error) {
      return (
        <CardActionFeedback
          type={this.state.connectionResult.type}
          message={T.translate(`${PREFIX}.ErrorMessages.${this.props.mode}`)}
          extendedMessage={this.state.error}
        />
      );
    }

    const connectionResultType = this.state.connectionResult.type;
    return (
      <CardActionFeedback
        message={T.translate(
          `${ADDCONN_PREFIX}.TestConnectionLabels.${connectionResultType.toLowerCase()}`
        )}
        extendedMessage={
          connectionResultType === CARD_ACTION_TYPES.SUCCESS
            ? null
            : this.state.connectionResult.message
        }
        type={connectionResultType}
      />
    );
  }

  render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="gcs-connection-modal cdap-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {
              connection: this.props.connectionId,
            })}
          </ModalHeader>

          <ModalBody>{this.renderContent()}</ModalBody>
          {this.renderAddConnectionButton()}
          {this.renderError()}
        </Modal>
      </div>
    );
  }
}

GCSConnection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string,
};
