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
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import DatabaseOptions from 'components/DataPrepConnections/DatabaseConnection/DatabaseOptions';
import DatabaseDetail from 'components/DataPrepConnections/DatabaseConnection/DatabaseDetail';
import NamespaceStore from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import MyDataPrepApi from 'api/dataprep';
import find from 'lodash/find';
import LoadingSVG from 'components/LoadingSVG';
import T from 'i18n-react';

require('./DatabaseConnection.scss');

const PREFIX = 'features.DataPrepConnections.AddConnections.Database';

export default class DatabaseConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      activeDB: null,
      connInfo: null,
      error: null,
      loading: false
    };

    this.add = this.add.bind(this);
  }

  componentWillMount() {
    if (this.props.mode !== 'ADD') {
      this.setState({loading: true});

      let namespace = NamespaceStore.getState().selectedNamespace;

      let params = {
        namespace,
        connectionId: this.props.connectionId
      };

      MyDataPrepApi.getConnection(params)
        .combineLatest(
          MyDataPrepApi.jdbcDrivers({ namespace }),
          MyDataPrepApi.jdbcAllowed(params)
        )
        .subscribe((res) => {
          let connInfo = objectQuery(res, 0, 'values', 0);
          let driverName = connInfo.properties.name;

          let pluginsList = objectQuery(res, 1, 'values');

          let matchedPlugin = find(pluginsList, (o) => {
            return o.properties.name === driverName;
          });

          if (!matchedPlugin) {
            this.setState({
              error: `Cannot find driver ${driverName}`,
              loading: false
            });

            return;
          }

          let pluginAllowed = objectQuery(res, 2, 'values');
          let matchedPluginAllowed = find(pluginAllowed, (o) => {
            return o.label === matchedPlugin.label;
          });

          let dbInfo = matchedPluginAllowed;
          dbInfo.pluginInfo = matchedPlugin;

          this.setState({
            connInfo,
            activeDB: dbInfo,
            loading: false
          });

        }, (err) => {
          console.log('err', err);
        });
    }

  }

  setActiveDB(db) {
    this.setState({activeDB: db});
  }

  renderDatabaseSelection() {
    return (
      <DatabaseOptions
        onDBSelect={this.setActiveDB.bind(this)}
      />
    );
  }

  renderDatabaseDetail() {
    return (
      <DatabaseDetail
        back={this.setActiveDB.bind(this, null)}
        db={this.state.activeDB}
        onAdd={this.add}
        mode={this.props.mode}
        connectionId={this.props.connectionId}
        connInfo={this.state.connInfo}
      />
    );
  }

  add() {
    this.props.onAdd();
    this.props.close();
  }

  render() {
    let content = this.state.activeDB ? this.renderDatabaseDetail() : this.renderDatabaseSelection();

    if (this.state.loading) {
      content = (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="database-connection-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {content}
          </ModalBody>
        </Modal>
      </div>
    );
  }
}

DatabaseConnection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string
};
