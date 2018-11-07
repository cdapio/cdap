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
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import DatabaseConnection from 'components/DataPrepConnections/DatabaseConnection';
import KafkaConnection from 'components/DataPrepConnections/KafkaConnection';
import S3Connection from 'components/DataPrepConnections/S3Connection';
import GCSConnection from 'components/DataPrepConnections/GCSConnection';
import BigQueryConnection from 'components/DataPrepConnections/BigQueryConnection';
import SpannerConnection from 'components/DataPrepConnections/SpannerConnection';
import T from 'i18n-react';
import find from 'lodash/find';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
require('./AddConnection.scss');

const PREFIX = 'features.DataPrepConnections.AddConnections';

export default class AddConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      activeModal: null,
      showPopover: this.props.showPopover,
    };

    this.CONNECTIONS_TYPE = [
      {
        type: ConnectionType.DATABASE,
        label: 'Database',
        icon: 'icon-database',
        component: DatabaseConnection,
      },
      {
        type: ConnectionType.KAFKA,
        label: 'Kafka',
        icon: 'icon-kafka',
        component: KafkaConnection,
      },
      {
        type: ConnectionType.S3,
        label: 'S3',
        icon: 'icon-s3',
        component: S3Connection,
      },
      {
        type: ConnectionType.GCS,
        label: 'Google Cloud Storage',
        icon: 'icon-storage',
        component: GCSConnection,
      },
      {
        type: ConnectionType.BIGQUERY,
        label: 'Google BigQuery',
        icon: 'icon-bigquery',
        component: BigQueryConnection,
      },
      {
        type: ConnectionType.SPANNER,
        label: 'Google Cloud Spanner',
        icon: 'icon-spanner',
        component: SpannerConnection,
      },
    ];
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.showPopover !== nextProps.showPopover) {
      this.setState({
        showPopover: nextProps.showPopover,
      });
    }
  }
  connectionClickHandler(component) {
    this.setState({
      activeModal: component,
      showPopover: false,
    });
  }

  renderModal() {
    if (!this.state.activeModal) {
      return null;
    }

    let ModalElem = this.state.activeModal;

    return (
      <ModalElem
        close={this.connectionClickHandler.bind(this, null)}
        onAdd={this.props.onAdd}
        mode="ADD"
      />
    );
  }

  onPopoverClose = (showPopover) => {
    if (!showPopover) {
      this.props.onPopoverClose();
    }
  };

  renderPopover() {
    const target = () => (
      <button className="btn btn-secondary">
        <span className="fa fa-fw">
          <IconSVG name="icon-plus" />
        </span>

        <span>{T.translate(`${PREFIX}.label`)}</span>
      </button>
    );

    return (
      <Popover
        placement="top"
        target={target}
        className="add-connection-popover"
        enableInteractionInPopover={true}
        showPopover={this.state.showPopover}
        onTogglePopover={this.onPopoverClose}
      >
        <div className="popover-header">{T.translate(`${PREFIX}.Popover.title`)}</div>
        <div className="popover-body">
          {this.CONNECTIONS_TYPE.map((connection) => {
            if (!find(this.props.validConnectionTypes, { type: connection.type })) {
              return null;
            }
            return (
              <div
                key={connection.label}
                className="connection-type-option"
                onClick={this.connectionClickHandler.bind(this, connection.component)}
              >
                <span className="fa fa-fw">
                  <IconSVG name={connection.icon} />
                </span>

                <span className="connection-name">{connection.label}</span>
              </div>
            );
          })}
        </div>
      </Popover>
    );
  }

  render() {
    return (
      <div className="add-connection-container text-center">
        {this.renderPopover()}
        {this.renderModal()}
      </div>
    );
  }
}

AddConnection.defaultProps = {
  showPopover: false,
};
AddConnection.propTypes = {
  onAdd: PropTypes.func,
  validConnectionTypes: PropTypes.arrayOf(PropTypes.object),
  showPopover: PropTypes.bool,
  onPopoverClose: PropTypes.func,
};
