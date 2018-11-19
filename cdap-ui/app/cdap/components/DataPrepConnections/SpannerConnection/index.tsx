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
import { getCurrentNamespace } from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import CardActionFeedback, { CARD_ACTION_TYPES } from 'components/CardActionFeedback';
import { objectQuery } from 'services/helpers';
import BtnWithLoading from 'components/BtnWithLoading';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';

const PREFIX = 'features.DataPrepConnections.AddConnections.Spanner';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-3 col-form-label text-right';
const INPUT_COL_CLASS = 'col-8';

require('./SpannerConnection.scss');

enum ConnectionMode {
  Add = 'ADD',
  Edit = 'EDIT',
  Duplicate = 'DUPLICATE',
}

interface ISpannerConnectionProps {
  close: () => void;
  onAdd: () => void;
  mode: ConnectionMode;
  connectionId: string;
}

interface ISpannerConnectionState {
  error?: string | object | null;
  name?: string;
  projectId?: string;
  serviceAccountKeyfile?: string;
  testConnectionLoading?: boolean;
  connectionResult?: {
    message?: string;
    type?: string;
  };
  loading?: boolean;
}

interface IProperties {
  projectId?: string;
  'service-account-keyfile'?: string;
}

export default class SpannerConnection extends React.PureComponent<
  ISpannerConnectionProps,
  ISpannerConnectionState
> {
  public state: ISpannerConnectionState = {
    error: null,
    name: '',
    projectId: '',
    serviceAccountKeyfile: '',
    testConnectionLoading: false,
    connectionResult: {
      message: '',
      type: '',
    },
    loading: false,
  };

  public componentDidMount() {
    if (this.props.mode === ConnectionMode.Add) {
      return;
    }

    this.setState({ loading: true });

    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    MyDataPrepApi.getConnection(params).subscribe(
      (res) => {
        const info = objectQuery(res, 'values', 0);
        const projectId = objectQuery(info, 'properties', 'projectId');
        const serviceAccountKeyfile = objectQuery(info, 'properties', 'service-account-keyfile');

        const name = this.props.mode === ConnectionMode.Edit ? info.name : '';

        this.setState({
          name,
          projectId,
          serviceAccountKeyfile,
          loading: false,
        });
      },
      (err) => {
        const error =
          objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;

        this.setState({
          loading: false,
          error,
        });
      }
    );
  }

  private constructProperties = (): IProperties => {
    const properties: IProperties = {};

    if (this.state.projectId && this.state.projectId.length > 0) {
      properties.projectId = this.state.projectId;
    }

    if (this.state.serviceAccountKeyfile && this.state.serviceAccountKeyfile.length > 0) {
      properties['service-account-keyfile'] = this.state.serviceAccountKeyfile;
    }

    return properties;
  };

  private addConnection = () => {
    const namespace = getCurrentNamespace();

    const requestBody = {
      name: this.state.name,
      type: ConnectionType.SPANNER,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.createConnection({ namespace }, requestBody).subscribe(
      () => {
        this.setState({ error: null });
        this.props.onAdd();
        this.props.close();
      },
      (err) => {
        const error =
          objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      }
    );
  };

  private editConnection = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    const requestBody = {
      name: this.state.name,
      id: this.props.connectionId,
      type: ConnectionType.SPANNER,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.updateConnection(params, requestBody).subscribe(
      () => {
        this.setState({ error: null });
        this.props.onAdd();
        this.props.close();
      },
      (err) => {
        const error =
          objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      }
    );
  };

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
      type: ConnectionType.SPANNER,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.spannerTestConnection({ namespace }, requestBody).subscribe(
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
        const errorMessage =
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
  };

  private handleChange = (key: string, e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({
      [key]: e.target.value,
    });
  };

  private renderTestButton = () => {
    const disabled = !this.state.name;

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
  };

  private renderAddConnectionButton = () => {
    const disabled = !this.state.name || this.state.testConnectionLoading;

    let onClickFn = this.addConnection;

    if (this.props.mode === ConnectionMode.Edit) {
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
  };

  private renderContent() {
    if (this.state.loading) {
      return (
        <div className="spanner-detail text-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="spanner-detail">
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
                  disabled={this.props.mode === ConnectionMode.Edit}
                  placeholder={T.translate(`${PREFIX}.Placeholders.name`).toString()}
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
                  placeholder={T.translate(`${PREFIX}.Placeholders.projectId`).toString()}
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
                  placeholder={T.translate(
                    `${PREFIX}.Placeholders.serviceAccountKeyfile`
                  ).toString()}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderMessage() {
    const connectionResult = this.state.connectionResult;

    if (!this.state.error && !connectionResult.message) {
      return null;
    }

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
    const extendedMessage =
      connectionResultType === CARD_ACTION_TYPES.SUCCESS ? null : connectionResult.message;

    return (
      <CardActionFeedback
        message={T.translate(
          `${ADDCONN_PREFIX}.TestConnectionLabels.${connectionResultType.toLowerCase()}`
        )}
        extendedMessage={extendedMessage}
        type={connectionResultType}
      />
    );
  }

  private renderModalFooter = () => {
    return this.renderAddConnectionButton();
  };

  public render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="spanner-connection-modal cdap-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {
              connection: this.props.connectionId,
            })}
          </ModalHeader>

          <ModalBody>{this.renderContent()}</ModalBody>

          {this.renderModalFooter()}
          {this.renderMessage()}
        </Modal>
      </div>
    );
  }
}
