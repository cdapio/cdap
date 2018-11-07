/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import UncontrolledPopover from 'components/UncontrolledComponents/Popover';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import LoadingSVG from 'components/LoadingSVG';
import DatabaseConnection from 'components/DataPrepConnections/DatabaseConnection';
import KafkaConnection from 'components/DataPrepConnections/KafkaConnection';
import S3Connection from 'components/DataPrepConnections/S3Connection';
import GCSConnection from 'components/DataPrepConnections/GCSConnection';
import BigQueryConnection from 'components/DataPrepConnections/BigQueryConnection';
import SpannerConnection from 'components/DataPrepConnections/SpannerConnection';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import CardActionFeedback from 'components/CardActionFeedback';
import If from 'components/If';
require('./ConnectionPopover.scss');

const PREFIX = 'features.DataPrepConnections.ConnectionManagement';

const COMPONENT_MAP = {
  [ConnectionType.DATABASE]: DatabaseConnection,
  [ConnectionType.KAFKA]: KafkaConnection,
  [ConnectionType.S3]: S3Connection,
  [ConnectionType.GCS]: GCSConnection,
  [ConnectionType.BIGQUERY]: BigQueryConnection,
  [ConnectionType.SPANNER]: SpannerConnection,
};

export default class ConnectionPopover extends Component {
  constructor(props) {
    super(props);

    this.state = {
      deleteConfirmation: false,
      extendedErrorMessage: null,
      error: null,
      edit: false,
      duplicate: false,
      loading: false,
    };

    this.delete = this.delete.bind(this);
    this.toggleDeleteConfirmation = this.toggleDeleteConfirmation.bind(this);
    this.toggleEdit = this.toggleEdit.bind(this);
    this.toggleDuplicate = this.toggleDuplicate.bind(this);
  }

  toggleDeleteConfirmation() {
    this.setState({ deleteConfirmation: !this.state.deleteConfirmation });
  }

  toggleEdit() {
    this.setState({ edit: !this.state.edit });
  }

  toggleDuplicate() {
    this.setState({ duplicate: !this.state.duplicate });
  }

  delete() {
    this.setState({ loading: true });

    let namespace = NamespaceStore.getState().selectedNamespace;
    let connectionId = this.props.connectionInfo.id;

    let params = {
      namespace,
      connectionId,
    };

    MyDataPrepApi.deleteConnection(params).subscribe(
      () => {
        this.setState({
          loading: false,
          deleteConfirmation: false,
        });

        this.props.onAction('delete', connectionId);
      },
      (err) => {
        let errMessage =
          objectQuery(err, 'message') ||
          objectQuery(err, 'response', 'message') ||
          objectQuery(err, 'response') ||
          null;

        this.setState({
          loading: false,
          error: T.translate(`${PREFIX}.Confirmations.failedDeleteMessage`),
          extendedErrorMessage: errMessage,
        });
      }
    );
  }

  renderDeleteConfirmationModal() {
    if (!this.state.deleteConfirmation) {
      return null;
    }

    let content;
    if (this.state.loading) {
      content = (
        <ModalBody>
          <div className="text-center">
            <LoadingSVG />
          </div>
        </ModalBody>
      );
    } else {
      content = (
        <ModalBody>
          <h4>
            {T.translate(`${PREFIX}.Confirmations.DatabaseDelete.mainMessage`, {
              connection: this.props.connectionInfo.name,
            })}
          </h4>

          <p>{T.translate(`${PREFIX}.Confirmations.DatabaseDelete.helper1`)}</p>
          <p>{T.translate(`${PREFIX}.Confirmations.DatabaseDelete.helper2`)}</p>

          <br />
        </ModalBody>
      );
    }

    return (
      <Modal
        backdrop="static"
        isOpen={true}
        toggle={this.toggleDeleteConfirmation}
        className="connection-delete-confirmation-modal cdap-modal"
        zIndex="1061"
      >
        <ModalHeader toggle={this.toggleDeleteConfirmation}>
          {T.translate(`${PREFIX}.Confirmations.DatabaseDelete.header`, {
            connection: this.props.connectionInfo.name,
          })}
        </ModalHeader>

        {content}
        <ModalFooter>
          <button className="btn btn-primary" onClick={this.delete}>
            {T.translate(`${PREFIX}.Confirmations.DatabaseDelete.deleteButton`)}
          </button>
          <button className="btn btn-secondary" onClick={this.toggleDeleteConfirmation}>
            {T.translate(`${PREFIX}.Confirmations.DatabaseDelete.cancel`)}
          </button>
        </ModalFooter>
        <If condition={this.state.error}>
          <CardActionFeedback
            type="DANGER"
            message={this.state.error}
            extendedMessage={this.state.extendedErrorMessage}
          />
        </If>
      </Modal>
    );
  }

  renderEdit() {
    if (!this.state.edit) {
      return null;
    }

    let Tag = COMPONENT_MAP[this.props.connectionInfo.type];

    return (
      <Tag
        close={this.toggleEdit}
        mode="EDIT"
        connectionId={this.props.connectionInfo.id}
        onAdd={this.props.onAction}
      />
    );
  }

  renderDuplicate() {
    if (!this.state.duplicate) {
      return null;
    }

    let Tag = COMPONENT_MAP[this.props.connectionInfo.type];

    return (
      <Tag
        close={this.toggleDuplicate}
        mode="DUPLICATE"
        connectionId={this.props.connectionInfo.id}
        onAdd={this.props.onAction}
      />
    );
  }

  render() {
    return (
      <span className="expanded-menu-popover-icon text-center float-right">
        <UncontrolledPopover icon="fa-ellipsis-v" popoverClassName="connection-action-popover">
          <div className="connection-action-item" onClick={this.toggleEdit}>
            <span>{T.translate(`${PREFIX}.edit`)}</span>
          </div>

          <div className="connection-action-item" onClick={this.toggleDuplicate}>
            <span>{T.translate(`${PREFIX}.duplicate`)}</span>
          </div>

          <div className="connection-action-item" onClick={this.toggleDeleteConfirmation}>
            <span>{T.translate(`${PREFIX}.delete`)}</span>
          </div>
        </UncontrolledPopover>

        {this.renderDeleteConfirmationModal()}
        {this.renderEdit()}
        {this.renderDuplicate()}
      </span>
    );
  }
}

// NEEDs TO BE UPDATED
ConnectionPopover.propTypes = {
  connectionInfo: PropTypes.object,
  onAction: PropTypes.func,
};
