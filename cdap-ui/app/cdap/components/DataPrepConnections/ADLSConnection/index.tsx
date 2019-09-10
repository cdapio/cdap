/*
 * Copyright © 2018 Cask Data, Inc.
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

import * as React from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import {getCurrentNamespace} from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import CardActionFeedback, {CARD_ACTION_TYPES} from 'components/CardActionFeedback';
import {objectQuery} from 'services/helpers';
import BtnWithLoading from 'components/BtnWithLoading';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';
import ValidatedInput from 'components/ValidatedInput';
import types from 'services/inputValidationTemplates';

const PREFIX = 'features.DataPrepConnections.AddConnections.ADLS';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-xs-3 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';

require('./ADLSConnection.scss');

enum ConnectionMode {
  Add = 'ADD',
  Edit = 'EDIT',
  Duplicate = 'DUPLICATE',
}

interface IADLSConnectionProps {
  close: () => void;
  onAdd: () => void;
  mode: ConnectionMode;
  connectionId: string;
}

interface IADLSConnectionState {
  error?: string | object | null;
  name?: string;
  kvURL?: string;
  clientIDKey?: string;
  clientSecretKey?: string;
  endPointURLKey?: string;
  accountFQDN?: string;
  clientID?: string;
  clientSecret?: string;
  refreshURL?: string;
  testConnectionLoading?: boolean;
  connectionResult?: {
    message?: string;
    type?: string
  };
  inputs?: object;
  loading?: boolean;
  isUsingJCEKfile?: boolean;
}

interface IProperties {
  kvURL?: string;
  clientIDKey?: string;
  clientSecretKey?: string;
  endPointURLKey?: string;
  accountFQDN?: string;
  clientID?: string;
  clientSecret?: string;
  refreshURL?: string;
  testConnectionLoading?: boolean;
}

const nameMap = 'name';
const accountFQDNMap = 'accountFQDN';
const kvURLMap = 'kvURL';
const clientIDKeyMap = 'clientIDKey';
const clientSecretKeyMap = 'clientSecretKey';
const endPointURLKeyMap = 'endPointURLKey';
const clientIDMap = 'clientID';
const clientSecretMap = 'clientSecret';
const refreshURLMap = 'refreshURL';

const errorMap = 'error';
const requiredMap = 'required';
const templateMap = 'template';
const labelMap = 'label';

export default class ADLSConnection extends React.PureComponent<IADLSConnectionProps, IADLSConnectionState> {
  public state: IADLSConnectionState = {
    error: null,
    name: '',
    kvURL: '',
    clientIDKey: '',
    clientSecretKey: '',
    endPointURLKey: '',
    clientID: '',
    clientSecret: '',
    refreshURL: '',
    accountFQDN: '',
    testConnectionLoading: false,
    connectionResult: {
      message: '',
      type: '',
    },
    isUsingJCEKfile: true,
    inputs: {
      name: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Connection Name',
      },
      accountFQDN: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Account FQDN',
      },
      kvURL: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'KeyVault URL',
      },
      clientIDKey: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Client ID',
      },
      clientSecretKey: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Client Secret Key',
      },
      endPointURLKey: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Tenant ID',
      },
      clientID: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Client ID',
      },
      clientSecret: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'Client Key',
      },
      refreshURL: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'End Point Refresh URL',
      },

    },
    loading: false,
  };

  public componentDidMount() {
    if (this.props.mode === ConnectionMode.Add) {
      return;
    }

    this.setState({loading: true});

    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        const info = objectQuery(res, 'values', 0);
        const name = this.props.mode === ConnectionMode.Edit ? info.name : '';
        const clientIDKey = objectQuery(info, 'properties', 'clientIDKey');
        const clientSecretKey = objectQuery(info, 'properties', 'clientSecretKey');
        const endPointURLKey = objectQuery(info, 'properties', 'endPointURLKey');
        const accountFQDN = objectQuery(info, 'properties', 'accountFQDN');
        const kvURL = objectQuery(info, 'properties', 'kvURL');
        const clientID = objectQuery(info, 'properties', 'clientID');
        const clientSecret = objectQuery(info, 'properties', 'clientSecret');
        const refreshURL = objectQuery(info, 'properties', 'refreshURL');
        const isUsingJCEKfile = kvURL !== undefined ? true : false;

        this.setState({
          name,
          kvURL,
          clientIDKey,
          clientSecretKey,
          endPointURLKey,
          accountFQDN,
          clientID,
          clientSecret,
          refreshURL,
          isUsingJCEKfile,
          loading: false,
        });
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;

        this.setState({
          loading: false,
          error,
        });
      });
  }

  private constructProperties = (): IProperties => {
    const properties: IProperties = {};

    if (this.state.isUsingJCEKfile) {
      if (this.state.kvURL && this.state.kvURL.length > 0) {
        properties.kvURL = this.state.kvURL;
      }
      if (this.state.clientIDKey && this.state.clientIDKey.length > 0) {
        properties.clientIDKey = this.state.clientIDKey;
      }
      if (this.state.clientSecretKey && this.state.clientSecretKey.length > 0) {
        properties.clientSecretKey = this.state.clientSecretKey;
      }
      if (this.state.endPointURLKey && this.state.endPointURLKey.length > 0) {
        properties.endPointURLKey = this.state.endPointURLKey;
      }
    } else {
      if (this.state.clientID && this.state.clientID.length > 0) {
        properties.clientID = this.state.clientID;
      }
      if (this.state.clientSecret && this.state.clientSecret.length > 0) {
        properties.clientSecret = this.state.clientSecret;
      }
      if (this.state.refreshURL && this.state.refreshURL.length > 0) {
        properties.refreshURL = this.state.refreshURL;
      }
    }

    if (this.state.accountFQDN && this.state.accountFQDN.length > 0) {
      properties.accountFQDN = this.state.accountFQDN;
    }

    return properties;
  }

  private addConnection = () => {
    const namespace = getCurrentNamespace();

    const requestBody = {
      name: this.state.name,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.createConnection({namespace}, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      });
  }

  private editConnection = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    const requestBody = {
      name: this.state.name,
      id: this.props.connectionId,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.updateConnection(params, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      });
  }

  private testConnection = () => {
    this.setState({
      testConnectionLoading: true,
      connectionResult: {
        message: '',
        type: '',
      },
      error: null,
    });

    const namespace = getCurrentNamespace();

    const requestBody = {
      name: this.state.name,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.adlsTestConnection({namespace}, requestBody)
      .subscribe((res) => {
        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.SUCCESS,
            message: res.message,
          },
          testConnectionLoading: false,
        });
      }, (err) => {
        const errorMessage = objectQuery(err, 'response', 'message') ||
        objectQuery(err, 'response') ||
        T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.DANGER,
            message: errorMessage,
          },
          testConnectionLoading: false,
        });
      });
  }

  private handleChange = (key: string, e: React.ChangeEvent<HTMLInputElement>) => {
    if (Object.keys(this.state.inputs).indexOf(key) > -1) {
      // validate input
      const isValid = types[this.state.inputs[key][templateMap]].validate(e.target.value);
      let errorMsg = '';
      if (e.target.value && !isValid) {
        errorMsg = types[this.state.inputs[key][templateMap]].getErrorMsg();
      }

      this.setState({
        [key]: e.target.value,
        inputs: {
          ...this.state.inputs,
          [key]: {
            ...this.state.inputs[key],
            error: errorMsg,
          },
        },
      });
    } else {
      this.setState({
        [key]: e.target.value,
      });
    }
  }

  private renderTestButton = () => {
    const disabled = this.isButtonDisabled();

    return (
      <span className="test-connection-button">
        <BtnWithLoading
          className="btn btn-secondary"
          onClick={this.testConnection}
          disabled={disabled}
          loading={this.state.testConnectionLoading}
          label={T.translate(`${PREFIX}.testConnection`)}
          darker={true}
        />
      </span>
    );
  }

  private isButtonDisabled = () => {
    let check;
    if (this.state.isUsingJCEKfile) {
      check = this.state.kvURL && this.state.clientIDKey && this.state.clientSecretKey && this.state.endPointURLKey;
    } else {
      check = this.state.clientID && this.state.clientSecret && this.state.refreshURL;
    }
    const disabled = !(this.state.name && check && this.state.accountFQDN) || !this.isValidInputs()
                      || this.state.testConnectionLoading;
    return disabled;
  }

  private isValidInputs = () => {
    if (this.state.inputs[nameMap].error !== '' || this.state.inputs[accountFQDNMap].error !== '') {
      return false;
    }

    if (this.state.isUsingJCEKfile) {
      if (this.state.inputs[kvURLMap].error !== '' || this.state.inputs[clientIDKeyMap].error !== ''
          || this.state.inputs[clientSecretKeyMap].error !== '' || this.state.inputs[endPointURLKeyMap].error !== '') {
        return false;
      }
    } else {
      if (this.state.inputs[clientIDMap].error !== '' || this.state.inputs[clientSecretMap].error !== ''
        || this.state.inputs[refreshURLMap].error !== '') {
        return false;
      }
    }

    return true;
  }

  private renderAddConnectionButton = () => {
    const disabled = this.isButtonDisabled();

    let onClickFn = this.addConnection;

    if (this.props.mode === ConnectionMode.Edit) {
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

  private handleOptionChange = (currentTarget) => {
    this.setState({isUsingJCEKfile : currentTarget.target.value === 'usingJCEKfile'});
  }

  private renderContent() {
    if (this.state.loading) {
      return (
        <div className="adls-detail text-xs-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }
    let renderOption;

    if (this.state.isUsingJCEKfile) {
      renderOption = (<div className='using-jcek-file'>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.kvURL`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[kvURLMap][labelMap]}
                inputInfo={types[this.state.inputs[kvURLMap][templateMap]].getInfo()}
                validationError={this.state.inputs[kvURLMap][errorMap]}
                className="form-control"
                value={this.state.kvURL || ''}
                onChange={this.handleChange.bind(this, 'kvURL')}
                placeholder={T.translate(`${PREFIX}.Placeholders.kvURL`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientIDKey`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[clientIDKeyMap][labelMap]}
                inputInfo={types[this.state.inputs[clientIDKeyMap][templateMap]].getInfo()}
                validationError={this.state.inputs[clientIDKeyMap][errorMap]}
                className="form-control"
                value={this.state.clientIDKey || ''}
                onChange={this.handleChange.bind(this, 'clientIDKey')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientIDKey`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientSecretKey`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[clientSecretKeyMap][labelMap]}
                inputInfo={types[this.state.inputs[clientSecretKeyMap][templateMap]].getInfo()}
                validationError={this.state.inputs[clientSecretKeyMap][errorMap]}
                className="form-control"
                value={this.state.clientSecretKey || ''}
                onChange={this.handleChange.bind(this, 'clientSecretKey')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientSecretKey`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.endPointURLKey`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[endPointURLKeyMap][labelMap]}
                inputInfo={types[this.state.inputs[endPointURLKeyMap][templateMap]].getInfo()}
                validationError={this.state.inputs[endPointURLKeyMap][errorMap]}
                className="form-control"
                value={this.state.endPointURLKey || ''}
                onChange={this.handleChange.bind(this, 'endPointURLKey')}
                placeholder={T.translate(`${PREFIX}.Placeholders.endPointURLKey`).toString()}
              />
            </div>
          </div>
        </div>
      </div>);
    } else {
      renderOption = (<div className='adls-gen1'>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientID`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[clientIDMap][labelMap]}
                inputInfo={types[this.state.inputs[clientIDMap][templateMap]].getInfo()}
                validationError={this.state.inputs[clientIDMap][errorMap]}
                className="form-control"
                value={this.state.clientID || '' }
                onChange={this.handleChange.bind(this, 'clientID')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientID`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientSecret`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[clientSecretMap][labelMap]}
                inputInfo={types[this.state.inputs[clientSecretMap][templateMap]].getInfo()}
                validationError={this.state.inputs[clientSecretMap][errorMap]}
                className="form-control"
                value={this.state.clientSecret || ''}
                onChange={this.handleChange.bind(this, 'clientSecret')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientSecret`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.refreshURL`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <ValidatedInput
                type="text"
                label={this.state.inputs[refreshURLMap][labelMap]}
                inputInfo={types[this.state.inputs[refreshURLMap][templateMap]].getInfo()}
                validationError={this.state.inputs[refreshURLMap][errorMap]}
                className="form-control"
                value={this.state.refreshURL || ''}
                onChange={this.handleChange.bind(this, 'refreshURL')}
                placeholder={T.translate(`${PREFIX}.Placeholders.refreshURL`).toString()}
              />
            </div>
          </div>
        </div>
      </div>);
    }

    return (
      <div className="adls-detail">
        <div className="form">
          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.name`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <ValidatedInput
                  type="text"
                  label={this.state.inputs[nameMap][labelMap]}
                  inputInfo={types[this.state.inputs[nameMap][templateMap]].getInfo()}
                  validationError={this.state.inputs[nameMap][errorMap]}
                  className="form-control"
                  value={this.state.name}
                  onChange={this.handleChange.bind(this, 'name')}
                  disabled={this.props.mode === ConnectionMode.Edit}
                  placeholder={T.translate(`${PREFIX}.Placeholders.name`).toString()}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.accountFQDN`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <ValidatedInput
                  type="text"
                  label={this.state.inputs[accountFQDNMap][labelMap]}
                  inputInfo={types[this.state.inputs[accountFQDNMap][templateMap]].getInfo()}
                  validationError={this.state.inputs[accountFQDNMap][errorMap]}
                  className="form-control"
                  value={this.state.accountFQDN}
                  onChange={this.handleChange.bind(this, 'accountFQDN')}
                  placeholder={T.translate(`${PREFIX}.Placeholders.accountFQDN`).toString()}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}></label>
            <div>
              <label className='radio-label'>
                <input
                  type="radio"
                  value="usingJCEKfile"
                  name="option"
                  checked={this.state.isUsingJCEKfile}
                  onChange={this.handleOptionChange.bind(this)}/>
                  {T.translate(`${PREFIX}.usingJcekFile`).toString()}
              </label>
              <label className='radio-label'>
                <input
                  type="radio"
                  value="clientID"
                  name="option"
                  checked={!this.state.isUsingJCEKfile}
                  onChange={this.handleOptionChange.bind(this)}/>
                  {T.translate(`${PREFIX}.adlsGen1Credentials`).toString()}
              </label>

            </div>
          </div>
          {renderOption}
        </div>
      </div>
    );
  }

  private renderMessage() {
    const connectionResult = this.state.connectionResult;

    if (!this.state.error && !connectionResult.message) { return null; }

    if (this.state.error) {
      return (
        <CardActionFeedback
          type={connectionResult.type}
          message={T.translate(`${PREFIX}.ErrorMessages.${this.props.mode}`)}
          extendedMessage={this.state.error}
        />
      );
    }

    const connectionResultType = connectionResult.type;
    const extendedMessage = connectionResultType === CARD_ACTION_TYPES.SUCCESS ? null : connectionResult.message;

    return (
      <CardActionFeedback
        message={T.translate(`${ADDCONN_PREFIX}.TestConnectionLabels.${connectionResultType.toLowerCase()}`)}
        extendedMessage={extendedMessage}
        type={connectionResultType}
      />
    );
  }

  private renderModalFooter = () => {
    return this.renderAddConnectionButton();
  }

  public render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="adls-connection-modal cdap-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {this.renderContent()}
          </ModalBody>

          {this.renderModalFooter()}
          {this.renderMessage()}
        </Modal>
      </div>
    );
  }
}
