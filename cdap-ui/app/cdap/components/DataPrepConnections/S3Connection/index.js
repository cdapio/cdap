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
import CardActionFeedback from 'components/CardActionFeedback';
import {objectQuery} from 'services/helpers';
import ee from 'event-emitter';

const PREFIX = 'features.DataPrepConnections.AddConnections.S3';

const LABEL_COL_CLASS = 'col-xs-3 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';

require('./S3Connection.scss');

export default class S3Connection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      error: null,
      name: '',
      accessKeyId: '',
      accessSecretKey: '',
      region: '',
      testConnectionLoading: false,
      connectionResult: null
    };

    this.eventEmitter = ee(ee);
    this.addConnection = this.addConnection.bind(this);
    this.editConnection = this.editConnection.bind(this);
    this.testConnection = this.testConnection.bind(this);
  }

  componentWillMount() {
    if (this.props.mode === 'ADD') { return; }

    this.setState({loading: true});

    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      connectionId: this.props.connectionId
    };

    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        let info = objectQuery(res, 'values', 0),
            accessKeyId = objectQuery(info, 'properties', 'accessKeyId'),
            accessSecretKey = objectQuery(info, 'properties', 'accessSecretKey'),
            region = objectQuery(info, 'properties', 'region');

        let name = this.props.mode === 'EDIT' ? info.name : '';

        this.setState({
          name,
          accessKeyId,
          accessSecretKey,
          region,
          loading: false
        });
      }, (err) => {
        console.log('failed to fetch connection detail', err);

        this.setState({
          loading: false
        });
      });
  }

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'S3',
      properties: {
        accessKeyId: this.state.accessKeyId,
        accessSecretKey: this.state.accessSecretKey,
        region: this.state.region
      }
    };

    MyDataPrepApi.createConnection({namespace}, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        console.log('err', err);

        let error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response');
        this.setState({ error });
      });
  }

  editConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      connectionId: this.props.connectionId
    };

    let requestBody = {
      name: this.state.name,
      id: this.props.connectionId,
      type: 'S3',
      properties: {
        accessKeyId: this.state.accessKeyId,
        accessSecretKey: this.state.accessSecretKey,
        region: this.state.region
      }
    };

    MyDataPrepApi.updateConnection(params, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.eventEmitter.emit('DATAPREP_CONNECTION_EDIT_S3', this.props.connectionId);
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        console.log('err', err);

        let error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response');
        this.setState({ error });
      });
  }

  testConnection() {
    this.setState({
      testConnectionLoading: true,
      connectionResult: null,
      error: null
    });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'S3',
      properties: {
        accessKeyId: this.state.accessKeyId,
        accessSecretKey: this.state.accessSecretKey,
        region: this.state.region
      }
    };

    MyDataPrepApi.s3TestConnection({namespace}, requestBody)
      .subscribe((res) => {
        this.setState({
          connectionResult: {
            type: 'success',
            message: res.message
          },
          testConnectionLoading: false
        });
      }, (err) => {
        console.log('Error testing kafka connection', err);

        let errorMessage = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: 'danger',
            message: errorMessage
          },
          testConnectionLoading: false
        });
      });
  }

  handleChange(key, e) {
    this.setState({
      [key]: e.target.value
    });
  }

  renderTestButton() {
    let disabled = !this.state.name ||
      !this.state.accessKeyId ||
      !this.state.accessSecretKey ||
      !this.state.region;

    return (
      <span className="test-connection-button">
        <button
          className="btn btn-secondary"
          onClick={this.testConnection}
          disabled={disabled}
        >
          {T.translate(`${PREFIX}.testConnection`)}
        </button>
        {
          this.state.testConnectionLoading ?
            (
              <span className="fa loading-indicator">
                <LoadingSVG />
              </span>
            )
          :
            null
        }
        {
          this.state.connectionResult ?
            (
              <span
                className={`connection-check text-${this.state.connectionResult.type}`}
              >
                {this.state.connectionResult.message}
              </span>
            )
          :
            null
        }
      </span>
    );
  }

  renderAddConnectionButton() {
    let disabled = !this.state.name ||
      !this.state.accessKeyId ||
      !this.state.accessSecretKey ||
      !this.state.region;

    let onClickFn = this.addConnection;

    if (this.props.mode === 'EDIT') {
      onClickFn = this.editConnection;
    }

    return (
      <div className="row">
        <div className="col-xs-9 offset-xs-3 col-xs-offset-3">
          <button
            className="btn btn-primary"
            onClick={onClickFn}
            disabled={disabled}
          >
            {T.translate(`${PREFIX}.Buttons.${this.props.mode}`)}
          </button>

          {this.renderTestButton()}
        </div>
      </div>
    );
  }

  renderContent() {
    if (this.state.loading) {
      return (
        <div className="s3-detail text-xs-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="s3-detail">
        <div className="row">
          <div className={`${INPUT_COL_CLASS} offset-xs-3 col-xs-offset-3`}>
            <span className="asterisk">*</span>
            <em>{T.translate(`${PREFIX}.required`)}</em>
          </div>
        </div>

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
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.accessKeyId`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.accessKeyId}
                  onChange={this.handleChange.bind(this, 'accessKeyId')}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.accessSecretKey`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.accessSecretKey}
                  onChange={this.handleChange.bind(this, 'accessSecretKey')}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.region`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.region}
                  onChange={this.handleChange.bind(this, 'region')}
                />
              </div>
            </div>
          </div>

          <br />

          {this.renderAddConnectionButton()}

        </div>
      </div>
    );
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <ModalFooter>
        <CardActionFeedback
          type="DANGER"
          message={T.translate(`${PREFIX}.ErrorMessages.${this.props.mode}`)}
          extendedMessage={this.state.error}
        />
      </ModalFooter>
    );
  }

  render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="s3-connection-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {this.renderContent()}
          </ModalBody>

          {this.renderError()}
        </Modal>
      </div>
    );
  }
}

S3Connection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string
};
