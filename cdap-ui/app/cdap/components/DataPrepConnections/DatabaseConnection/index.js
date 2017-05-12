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
import DatabaseOptions from 'components/DataPrepConnections/DatabaseConnection/DatabaseOptions';
import DatabaseDetail from 'components/DataPrepConnections/DatabaseConnection/DatabaseDetail';
import T from 'i18n-react';

require('./DatabaseConnection.scss');

const PREFIX = 'features.DataPrepConnections.AddConnections.Database';

export default class DatabaseConnection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      activeDB: null
    };

    this.add = this.add.bind(this);
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
      />
    );
  }

  add() {
    this.props.onAdd();
    this.props.close();
  }

  render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="database-connection-modal"
          backdrop="static"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.modalHeader`)}
          </ModalHeader>

          <ModalBody>
            {
              this.state.activeDB ?
                this.renderDatabaseDetail()
              :
                this.renderDatabaseSelection()
            }
          </ModalBody>
        </Modal>
      </div>
    );
  }
}

DatabaseConnection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func
};
