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
import IconSVG from 'components/IconSVG';
import { Popover, PopoverTitle, PopoverContent } from 'reactstrap';
import DatabaseConnection from 'components/DataPrepConnections/DatabaseConnection';
import KafkaConnection from 'components/DataPrepConnections/KafkaConnection';
import T from 'i18n-react';

require('./AddConnection.scss');

const ADD_CONNECTION_ID = 'add-connection-button';
const PREFIX = 'features.DataPrepConnections.AddConnections';

export default class AddConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      expanded: false,
      activeModal: null
    };

    this.toggleConnectionOptions = this.toggleConnectionOptions.bind(this);

    this.CONNECTIONS_TYPE = [
      {
        type: 'Database',
        icon: 'icon-database',
        component: DatabaseConnection
      },
      // {
      //   type: 'S3',
      //   icon: 'icon-s3'
      // },
      {
        type: 'Kafka',
        icon: 'icon-kafka',
        component: KafkaConnection
      }
    ];
  }

  toggleConnectionOptions() {
    this.setState({expanded: !this.state.expanded});
  }

  connectionClickHandler(component) {
    this.setState({
      activeModal: component,
      expanded: false
    });
  }

  renderModal() {
    if (!this.state.activeModal) { return null; }

    let ModalElem = this.state.activeModal;

    return (
      <ModalElem
        close={this.connectionClickHandler.bind(this, null)}
        onAdd={this.props.onAdd}
        mode="ADD"
      />
    );
  }

  renderPopover() {
    const tetherConfig = {
      classes: {
        element: 'add-connection-popover'
      }
    };

    return (
      <Popover
        placement="top"
        isOpen={this.state.expanded}
        target={ADD_CONNECTION_ID}
        toggle={this.toggleConnectionOptions}
        tether={tetherConfig}
      >
        <PopoverTitle>
          {T.translate(`${PREFIX}.Popover.title`)}
        </PopoverTitle>
        <PopoverContent>
          {
            this.CONNECTIONS_TYPE.map((connection) => {
              return (
                <div
                  key={connection.type}
                  className="connection-type-option"
                  onClick={this.connectionClickHandler.bind(this, connection.component)}
                >
                  <span className="fa fa-fw">
                    <IconSVG name={connection.icon} />
                  </span>

                  <span className="connection-name">
                    {connection.type}
                  </span>
                </div>
              );
            })
          }
        </PopoverContent>
      </Popover>
    );
  }

  render() {
    return (
      <div className="add-connection-container text-xs-center">
        <button
          className="btn btn-secondary"
          onClick={this.toggleConnectionOptions}
          id={ADD_CONNECTION_ID}
        >
          <span className="fa fa-fw">
            <IconSVG name="icon-plus" />
          </span>

          <span>
            {T.translate(`${PREFIX}.label`)}
          </span>
        </button>

        {this.renderPopover()}
        {this.renderModal()}
      </div>
    );
  }
}

AddConnection.propTypes = {
  onAdd: PropTypes.func
};

