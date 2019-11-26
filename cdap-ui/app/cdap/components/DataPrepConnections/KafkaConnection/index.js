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
import {objectQuery, isNilOrEmpty} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import HostPortEditor from 'components/DataPrepConnections/KafkaConnection/HostPortEditor';
import uuidV4 from 'uuid/v4';
import ee from 'event-emitter';
import CardActionFeedback, {CARD_ACTION_TYPES} from 'components/CardActionFeedback';
import BtnWithLoading from 'components/BtnWithLoading';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import KeyValuePairs from 'components/KeyValuePairs';
import ValidatedInput from 'components/ValidatedInput';
import types from 'services/inputValidationTemplates';
import { Theme } from 'services/ThemeHelper';

const PREFIX = 'features.DataPrepConnections.AddConnections.Kafka';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-xs-3 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';
const DEFAULT_KAFKA_PRODUCER_PROPERTIES = {
  'pairs': [{
    'key': '',
    'value': '',
    'validKey': true,
    'validValue': true,
    'uniqueId': uuidV4()
  }]
};

require('./KafkaConnection.scss');

export default class KafkaConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      name: '',
      brokersList: [{
        host: 'localhost',
        port: '9062',
        uniqueId: uuidV4(),
        valid: true,
      }],
      principal: '',
      keytabLocation: '',
      kafkaProducerProperties: DEFAULT_KAFKA_PRODUCER_PROPERTIES,
      connectionResult: {
        type: null,
        message: null
      },
      testConnectionLoading: false,
      error: null,
      inputs: {
        'name': {
          'error': '',
          'required': true,
          'template': 'NAME',
          'label': T.translate(`${PREFIX}.name`)
        },
        'principal': {
          'error': '',
          'template': 'KERBEROS_PRINCIPAL',
          'label': T.translate(`${PREFIX}.principal`)
        },
        'keytabLocation': {
          'error': '',
          'template': 'KEYTAB_LOCATION',
          'label': T.translate(`${PREFIX}.keytabLocation`)
        }
      },
      loading: false
    };

    this.eventEmitter = ee(ee);
    this.preventDefault = this.preventDefault.bind(this);
    this.addConnection = this.addConnection.bind(this);
    this.editConnection = this.editConnection.bind(this);
    this.testConnection = this.testConnection.bind(this);
    this.handleBrokersChange = this.handleBrokersChange.bind(this);
    this.onKeyValueChange = this.onKeyValueChange.bind(this);

  }

  componentWillMount() {
    if (this.props.mode === 'ADD') { return; }

    this.setState({ loading: true });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      connectionId: this.props.connectionId
    };

    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        let info = objectQuery(res, 'values', 0),
          brokers = objectQuery(info, 'properties', 'brokers'),
          kafkaProducerPropertiesPairs = objectQuery(info, 'properties', 'kafkaProducerProperties');

        let name = this.props.mode === 'EDIT' ? info.name : '';
        let brokersList = this.parseBrokers(brokers);
        let principal = objectQuery(info, 'properties', 'principal');
        let keytabLocation = objectQuery(info, 'properties', 'keytabLocation');
        let kafkaProducerProperties = {'pairs': this.getKeyValPair(JSON.parse(kafkaProducerPropertiesPairs))};
        this.setState({
          name,
          brokersList,
          principal,
          keytabLocation,
          kafkaProducerProperties,
          loading: false
        });
      }, (err) => {
        console.log('failed to fetch connection detail', err);

        this.setState({
          loading: false
        });
      });
  }

  getKeyValObject() {
    let keyValArr = this.state.kafkaProducerProperties ? this.state.kafkaProducerProperties.pairs : DEFAULT_KAFKA_PRODUCER_PROPERTIES.pairs;
    let keyValObj = {};
    keyValArr.forEach((pair) => {
      if (pair.key.length > 0 && pair.value.length > 0) {
        keyValObj[pair.key] = pair.value;
      }
    });
    return keyValObj;
  }

  getKeyValPair(prefObj) {
    let sortedPrefObjectKeys = [...Object.keys(prefObj)].sort();
    if (isNilOrEmpty(sortedPrefObjectKeys)) {
      return DEFAULT_KAFKA_PRODUCER_PROPERTIES.pairs;
    } else {
      return sortedPrefObjectKeys.map(key => {
        return {
          key: key,
          value: prefObj[key],
          uniqueId: uuidV4(),
          'validKey':true,
          'validValue':true,
        };
      });
    }
  }

  parseBrokers(brokers) {
    let brokersList = [];

    brokers.split(',').forEach((broker) => {
      let split = broker.trim().split(':');

      let obj = {
        host: split[0] || '',
        port: split[1] || '',
        uniqueId: uuidV4(),
        valid: true
      };

      brokersList.push(obj);
    });

    return brokersList;
  }

  preventDefault(e) {
    e.preventDefault();
    e.stopPropagation();
  }

  handleBrokersChange(rows) {
    this.setState({
      brokersList: rows
    });
  }

  convertBrokersList() {
    return this.state.brokersList.map((broker) => {
      return `${broker.host}:${broker.port}`;
    }).join(',');
  }

  getProperties() {
    const prop = {
      brokers: this.convertBrokersList(),
      kafkaProducerProperties: JSON.stringify(this.getKeyValObject())
    };
    return Theme.isCustomerJIO ? prop :
      {
        ...prop,
        principal: this.state.principal,
        keytabLocation: this.state.keytabLocation,
      };
  }
  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.KAFKA,
      properties: this.getProperties()
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
      type: ConnectionType.KAFKA,
      properties: this.getProperties()
    };

    MyDataPrepApi.updateConnection(params, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.eventEmitter.emit('DATAPREP_CONNECTION_EDIT_KAFKA', this.props.connectionId);
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        console.log('err', err);

        let error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response');
        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.DANGER,
            message: error
          },
          testConnectionLoading: false
        });
      });
  }

  testConnection() {
    this.setState({
      testConnectionLoading: true,
      connectionResult: {
        type: null,
        message: null
      },
      error: null
    });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.KAFKA,
      properties: this.getProperties()
    };

    MyDataPrepApi.kafkaTestConnection({namespace}, requestBody)
      .subscribe((res) => {
        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.SUCCESS,
            message: res.message
          },
          testConnectionLoading: false
        });
      }, (err) => {
        console.log('Error testing kafka connection', err);

        let errorMessage = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.DANGER,
            message: errorMessage
          },
          testConnectionLoading: false
        });
      });
  }

  /** Return true if there is some error. */
  testInputs() {
    let isSomeErrorInputs = Object.keys(this.state.inputs).some(key => this.state.inputs[key]['error'] !== '');
    let kafkaProducerProperties = this.state.kafkaProducerProperties ? this.state.kafkaProducerProperties.pairs : DEFAULT_KAFKA_PRODUCER_PROPERTIES.pairs;
    let isSomeErrorKeyValuePairs = kafkaProducerProperties.some(property => { return (!property.validKey || !property.validValue);});
    return isSomeErrorInputs || isSomeErrorKeyValuePairs;
  }

  handleChange(key, e) {
    if (Object.keys(this.state.inputs).includes(key)) {
      // validate input
      const isValid = types[this.state.inputs[key]['template']].validate(e.target.value);
      let errorMsg = '';
      if (e.target.value && !isValid) {
        errorMsg = types[this.state.inputs[key]['template']].getErrorMsg();
      }

      this.setState({
        [key]: e.target.value,
        inputs: {
          ...this.state.inputs,
          [key]: {
            ...this.state.inputs[key],
            'error': errorMsg
          }
        }
      });
    } else {
      this.setState({
        [key]: e.target.value
      });
    }
  }

  renderKafka() {
    return (
      <div className="form-group row">
        <label className={LABEL_COL_CLASS}>
          {T.translate(`${PREFIX}.brokersList`)}
          <span className="asterisk">*</span>
        </label>
        <div className={INPUT_COL_CLASS}>
          <HostPortEditor
            values={this.state.brokersList}
            onChange={this.handleBrokersChange}
          />
        </div>
      </div>
    );
  }

  renderAddConnectionButton() {
    let disabled = this.testInputs() || !this.state.name;
    disabled = disabled ||
      this.state.brokersList.length === 0 ||
      this.state.testConnectionLoading ||
      (
        this.state.brokersList.length === 1 &&
        (!this.state.brokersList[0].host || !this.state.brokersList[0].port)
      );

    let onClickFn = this.addConnection;

    if (this.props.mode === 'EDIT') {
      onClickFn = this.editConnection;
    }

    return (
      <ModalFooter>
        <button
          className="btn btn-primary"
          onClick={onClickFn}
          disabled={disabled}
        >
          {T.translate(`${PREFIX}.Buttons.${this.props.mode}`)}
        </button>

        {this.renderTestButton()}
      </ModalFooter>
    );
  }

  renderTestButton() {
    let disabled = this.testInputs() || this.state.testConnectionLoading || !this.state.name;
    disabled = disabled || this.state.brokersList.length === 0 || (this.state.brokersList.length === 1 && (!this.state.brokersList[0].host || !this.state.brokersList[0].port));

    return (
      <BtnWithLoading
        className="btn btn-secondary"
        onClick={this.testConnection}
        disabled={disabled}
        label={T.translate(`${PREFIX}.testConnection`)}
        loading={this.state.testConnectionLoading}
        darker={true}
      />
    );
  }

  renderError() {
    if (!this.state.error && !this.state.connectionResult.message) { return null; }

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
        message={T.translate(`${ADDCONN_PREFIX}.TestConnectionLabels.${connectionResultType.toLowerCase()}`)}
        extendedMessage={connectionResultType === CARD_ACTION_TYPES.SUCCESS ? null : this.state.connectionResult.message}
        type={connectionResultType}
      />
    );
  }

  renderKafkaProducerProperties() {
    return (
      <div className="form-group row">
        <label className={LABEL_COL_CLASS}>
          {T.translate(`${PREFIX}.kafkaProducerProperties`)}
        </label>
        <div className={`${INPUT_COL_CLASS} kafka-producer-prop-container`}>
          <KeyValuePairs
            keyValues = {this.state.kafkaProducerProperties}
            onKeyValueChange = {this.onKeyValueChange}
          />
        </div>
      </div>
    );
  }

  onKeyValueChange(kafkaProducerProperties) {
    this.setState({kafkaProducerProperties});
  }

  renderPrincipalKeytabLocation() {
    return (
      <div>
        {/* principal field */}
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.principal`)}
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-name">
              <ValidatedInput
                type="text"
                label={this.state.inputs['principal']['label']}
                validationError={this.state.inputs['principal']['error']}
                className="form-control"
                value={this.state.principal}
                onChange={this.handleChange.bind(this, 'principal')}
                placeholder={T.translate(`${PREFIX}.Placeholders.principal`)}
              />
            </div>
          </div>
        </div>

        {/* keytabLocation field */}
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.keytabLocation`)}
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-name">
              <ValidatedInput
                type="text"
                label={this.state.inputs['keytabLocation']['label']}
                validationError={this.state.inputs['keytabLocation']['error']}
                className="form-control"
                value={this.state.keytabLocation}
                onChange={this.handleChange.bind(this, 'keytabLocation')}
                placeholder={T.translate(`${PREFIX}.Placeholders.keytabLocation`)}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }
  renderContent() {
    if (this.state.loading) {
      return (
        <div className="kafka-detail text-xs-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="kafka-detail">

        <div className="form">
          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.name`)}
              { this.state.inputs['name']['required'] &&
                <span className="asterisk">*</span>
              }
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-name">
                <ValidatedInput
                  type="text"
                  label={this.state.inputs['name']['label']}
                  validationError={this.state.inputs['name']['error']}
                  className="form-control"
                  value={this.state.name}
                  onChange={this.handleChange.bind(this, 'name')}
                  disabled={this.props.mode === 'EDIT'}
                  placeholder={T.translate(`${PREFIX}.Placeholders.name`)}
                />
              </div>
            </div>
          </div>

          {this.renderKafka()}
          {
            Theme.isCustomerJIO ? null : this.renderPrincipalKeytabLocation()
          }
          {this.renderKafkaProducerProperties()}
        </div>
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
          className="kafka-connection-modal cdap-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {this.renderContent()}
          </ModalBody>

          {this.renderAddConnectionButton()}
          {this.renderError()}
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
