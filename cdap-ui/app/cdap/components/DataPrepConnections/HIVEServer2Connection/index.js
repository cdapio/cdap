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
import HIVEServer2Detail from 'components/DataPrepConnections/HIVEServer2Connection/HIVEServer2Detail';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import MyDataPrepApi from 'api/dataprep';
import LoadingSVG from 'components/LoadingSVG';
import T from 'i18n-react';

require('./HIVEServer2Connection.scss');

const PREFIX = 'features.DataPrepConnections.AddConnections.HIVEServer2';
const ConnectionMode = {
  Add: 'ADD',
  Edit: 'EDIT',
  Duplicate: 'DUPLICATE',
};
export default class HIVEServer2Connection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      name: null,
      url:'',
      error: null,
      loading: false,
      database: ''
    };

    this.add = this.add.bind(this);
  }

  componentWillMount() {
    if (this.props.mode === 'ADD') {
      return;
    }
    this.setState({loading: true});
    const namespace = getCurrentNamespace();
    let params = {
      namespace,
      connectionId: this.props.connectionId
    };
    MyDataPrepApi.getConnection(params)
    .subscribe((res) => {
      let connInfo = objectQuery(res, 'values', 0);
      let name = connInfo.name;
      let url = objectQuery(connInfo, 'properties', 'url');
      let database = objectQuery(connInfo, 'properties', 'database');

      this.setState({
        name,
        url,
        database,
        loading: false
      });

    }, (err) => {
      console.log('err', err);
    });
  }

  renderHIVEServer2Detail() {
    const obj = {
      name: this.state.name,
      url: this.state.url,
      database: this.state.database
    };
    return (
      <HIVEServer2Detail
        db={obj}
        onAdd={this.add}
        onClose={this.close}
        mode={this.props.mode}
        connectionId={this.props.connectionId}
      />
    );
  }

  add() {
    this.props.onAdd();
    this.props.close();
  }

  render() {
    let content = this.renderHIVEServer2Detail();

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
          className="hive-server2-connection-modal cdap-modal"
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

HIVEServer2Connection.propTypes = {
  close: PropTypes.func,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf([ConnectionMode.Add, ConnectionMode.Edit, ConnectionMode.Duplicate]).isRequired,
  connectionId: PropTypes.string
};
