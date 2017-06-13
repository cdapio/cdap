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
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';

const PREFIX = 'features.DataPrepConnections.AddConnections.Kafka';

const LABEL_COL_CLASS = 'col-xs-3 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';

require('./KafkaConnection.scss');

export default class KafkaConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      name: '',
      brokers: '',
      brokersPort: '',
      connectionResult: null,
      testConnectionLoading: false,
      error: null
    };

    this.preventDefault = this.preventDefault.bind(this);
    this.addConnection = this.addConnection.bind(this);
    this.editConnection = this.editConnection.bind(this);
    this.testConnection = this.testConnection.bind(this);
  }

  preventDefault(e) {
    e.preventDefault();
  }

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'KAFKA',
      properties: {
        brokers: `${this.state.brokers}:${this.state.brokersPort}`
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

  }

  testConnection() {
    this.setState({testConnectionLoading: true});

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'KAFKA',
      properties: {
        brokers: `${this.state.brokers}:${this.state.brokersPort}`
      }
    };

    console.log('test', requestBody);

    MyDataPrepApi.kafkaTestConnection({namespace}, requestBody)
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

  renderKafka() {
    return (
      <div className="form-group row">
        <label className={LABEL_COL_CLASS}>
          {T.translate(`${PREFIX}.brokersList`)}
          <span className="asterisk">*</span>
        </label>
        <div className="col-xs-5">
          <input
            type="text"
            className="form-control"
            value={this.state.brokers}
            onChange={this.handleChange.bind(this, 'brokers')}
          />
        </div>
        <label className="col-xs-1 col-form-label text-xs-right">
          {T.translate(`${PREFIX}.port`)}
        </label>
        <div className="col-xs-2">
          <input
            type="text"
            className="form-control"
            value={this.state.brokersPort}
            onChange={this.handleChange.bind(this, 'brokersPort')}
          />
        </div>
      </div>
    );
  }

  renderAddConnectionButton() {
    let disabled = !this.state.name;
    disabled = disabled || !this.state.brokers || !this.state.brokersPort;

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

  renderTestButton() {
    let disabled = this.state.testConnectionLoading;

    return (
      <span>
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

  renderContent() {
    return (
      <div className="kafka-detail">
        <div className="row">
          <div className={`${INPUT_COL_CLASS} offset-xs-3 col-xs-offset-3`}>
            <span className="asterisk">*</span>
            <em>{T.translate(`${PREFIX}.required`)}</em>
          </div>
        </div>

        <form onSubmit={this.preventDefault}>
          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.name`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <input
                type="text"
                className="form-control"
                value={this.state.name}
                onChange={this.handleChange.bind(this, 'name')}
                disabled={this.props.mode === 'EDIT'}
              />
            </div>
          </div>

          {this.renderKafka()}

          {this.renderAddConnectionButton()}

        </form>
      </div>
    );
  }

  render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="kafka-connection-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {this.renderContent()}
          </ModalBody>
        </Modal>
      </div>
    );
  }
}

KafkaConnection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string
};
