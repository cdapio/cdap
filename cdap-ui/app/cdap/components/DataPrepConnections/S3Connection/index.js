/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import BtnWithLoading from 'components/BtnWithLoading';
import ee from 'event-emitter';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import { WrappedWidgetWrapper } from 'components/ConfigurationGroup/WidgetWrapper';

const PREFIX = 'features.DataPrepConnections.AddConnections.S3';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-3 col-form-label text-right';
const INPUT_COL_CLASS = 'col-8';

const REGIONS = [
  {
    name: '',
    value: '',
  },
  {
    name: 'US East (Ohio)',
    value: 'us-east-2',
  },
  {
    name: 'US East (N. Virginia)',
    value: 'us-east-1',
  },
  {
    name: 'US West (N. California)',
    value: 'us-west-1',
  },
  {
    name: 'US West (Oregon)',
    value: 'us-west-2',
  },
  {
    name: 'Asia Pacific (Mumbai)',
    value: 'ap-south-1',
  },
  {
    name: 'Asia Pacific (Seoul)',
    value: 'ap-northeast-2',
  },
  {
    name: 'Asia Pacific (Singapore)',
    value: 'ap-southeast-1',
  },
  {
    name: 'Asia Pacific (Sydney)',
    value: 'ap-southeast-2',
  },
  {
    name: 'Asia Pacific (Tokyo)',
    value: 'ap-northeast-1',
  },
  {
    name: 'Canada (Central)',
    value: 'ca-central-1',
  },
  {
    name: 'EU (Frankfurt)',
    value: 'eu-central-1',
  },
  {
    name: 'EU (Ireland)',
    value: 'eu-west-1',
  },
  {
    name: 'EU (London)',
    value: 'eu-west-2',
  },
  {
    name: 'South America (São Paulo)',
    value: 'sa-east-1',
  },
];

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
      connectionResult: {
        type: null,
        message: null,
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
        const accessKeyId = objectQuery(res, 'properties', 'accessKeyId'),
          accessSecretKey = objectQuery(res, 'properties', 'accessSecretKey'),
          region = objectQuery(res, 'properties', 'region');

        let name = this.props.mode === 'EDIT' ? res.name : '';

        this.setState({
          name,
          accessKeyId,
          accessSecretKey,
          region,
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

  constructProperties() {
    return {
      accessKeyId: this.state.accessKeyId.trim(),
      accessSecretKey: this.state.accessSecretKey.trim(),
      region: this.state.region.trim(),
    };
  }

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.S3,
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
      type: ConnectionType.S3,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.updateConnection(params, requestBody).subscribe(
      () => {
        this.setState({ error: null });
        this.eventEmitter.emit('DATAPREP_CONNECTION_EDIT_S3', this.props.connectionId);
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
        type: null,
        message: null,
      },
      error: null,
    });

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: ConnectionType.S3,
      properties: {
        accessKeyId: this.state.accessKeyId,
        accessSecretKey: this.state.accessSecretKey,
        region: this.state.region,
      },
    };

    MyDataPrepApi.s3TestConnection({ context: namespace }, requestBody).subscribe(
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
        console.log('Error testing S3 connection', err);

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

  handleAccessSecretChange = (property) => {
    return (value) => {
      this.setState({
        [property]: value,
      });
    };
  };

  renderTestButton() {
    let disabled =
      !this.state.name ||
      !this.state.accessKeyId ||
      !this.state.accessSecretKey ||
      !this.state.region;

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

  renderAddConnectionButton() {
    let disabled =
      !this.state.name ||
      !this.state.accessKeyId ||
      !this.state.accessSecretKey ||
      !this.state.region;

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
        <div className="s3-detail text-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="s3-detail">
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
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.accessKeyId`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <WrappedWidgetWrapper
                  widgetProperty={{
                    'widget-type': 'securekey-text',
                    'widget-attributes': {
                      placeholder: T.translate(`${PREFIX}.Placeholders.accessKeyId`).toString(),
                    },
                  }}
                  value={this.state.accessKeyId}
                  onChange={this.handleAccessSecretChange('accessKeyId')}
                  hideLabel={true}
                  hideDescription={true}
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
                <WrappedWidgetWrapper
                  widgetProperty={{
                    'widget-type': 'securekey-password',
                    'widget-attributes': {
                      placeholder: T.translate(`${PREFIX}.Placeholders.accessSecretKey`).toString(),
                    },
                  }}
                  value={this.state.accessSecretKey}
                  onChange={this.handleAccessSecretChange('accessSecretKey')}
                  hideLabel={true}
                  hideDescription={true}
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
                <select
                  className="form-control"
                  value={this.state.region}
                  onChange={this.handleChange.bind(this, 'region')}
                >
                  {REGIONS.map((region) => {
                    return (
                      <option value={region.value} key={region.value}>
                        {region.name} - {region.value}
                      </option>
                    );
                  })}
                </select>
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
          type={CARD_ACTION_TYPES.DANGER}
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
          className="s3-connection-modal cdap-modal"
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

S3Connection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string,
};
