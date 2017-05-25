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
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import classnames from 'classnames';
import T from 'i18n-react';
import replace from 'lodash/replace';
import ee from 'event-emitter';
import {objectQuery} from 'services/helpers';

const CONN_TYPE = {
  basic: 'BASIC',
  advanced: 'ADVANCED'
};

const LABEL_COL_CLASS = 'col-xs-4 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';

const PREFIX = 'features.DataPrepConnections.AddConnections.Database.DatabaseDetail';

export default class DatabaseDetail extends Component {
  constructor(props) {
    super(props);

    this.state = {
      connType: CONN_TYPE.basic,
      name: '',
      hostname: '',
      port: '',
      username: '',
      password: '',
      database: '',
      connectionString: '',
      connectionResult: null
    };

    this.eventEmitter = ee(ee);
    this.addConnection = this.addConnection.bind(this);
    this.editConnection = this.editConnection.bind(this);
    this.testConnection = this.testConnection.bind(this);
    this.preventDefault = this.preventDefault.bind(this);
  }

  componentWillMount() {
    if (this.props.mode !== 'ADD') {
      let name = this.props.mode === 'EDIT' ? this.props.connInfo.name : '';

      let {
        connectionString = '',
        password = '',
        username = '',
        hostname = '',
        port = '',
        database = ''
      } = this.props.connInfo.properties;

      this.setState({
        name,
        connectionString,
        password,
        username,
        hostname,
        port,
        database,
        connType: connectionString ? CONN_TYPE.advanced : CONN_TYPE.basic
      });
    }
  }

  preventDefault(e) {
    e.preventDefault();
  }

  handleChange(key, e) {
    this.setState({
      [key]: e.target.value,
      connectionResult: null
    });
  }

  handleConnTypeChange(type) {
    this.setState({connType: type});
  }

  constructProperties() {
    let properties;
    let db = this.props.db;

    if (this.state.connType === CONN_TYPE.basic) {
      properties = {
        hostname: this.state.hostname,
        port: this.state.port,
        username: this.state.username,
        password: this.state.password,
        database: this.state.database
      };
    } else {
      properties = {
        connectionString: this.state.connectionString,
        username: this.state.username,
        password: this.state.password
      };
    }

    properties.name = db.name;
    properties.type = db.pluginInfo.properties.type;
    properties.class =  db.pluginInfo.properties.class;
    properties.url = this.interpolateConnectionString(db.pluginInfo.url);

    return properties;
  }

  interpolateConnectionString(url) {
    let state = this.state;
    let required = this.props.db.pluginInfo.fields;

    let interpolatedUrl = url;

    if (this.state.connType === CONN_TYPE.basic) {
      required.forEach((field) => {
        interpolatedUrl = replace(interpolatedUrl, '${' + field + '}', state[field]);
      });
    } else {
      interpolatedUrl = this.state.connectionString;
    }

    return interpolatedUrl;
  }

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'DATABASE',
      properties: this.constructProperties()
    };

    MyDataPrepApi.createConnection({namespace}, requestBody)
      .subscribe(() => {
        this.props.onAdd();
      }, (err) => {
        console.log('err', err);
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
      type: 'DATABASE',
      properties: this.constructProperties()
    };

    MyDataPrepApi.updateConnection(params, requestBody)
      .subscribe(() => {
        this.eventEmitter.emit('DATAPREP_CONNECTION_EDIT_DATABASE', this.props.connectionId);
        this.props.onAdd();
      }, (err) => {
        console.log('err', err);
      });
  }

  testConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestBody = {
      name: this.state.name,
      type: 'DATABASE',
      properties: this.constructProperties()
    };

    MyDataPrepApi.jdbcTestConnection({namespace}, requestBody)
      .subscribe((res) => {
        this.setState({
          connectionResult: {
            type: 'success',
            message: res.message
          }
        });
      }, (err) => {
        console.log('Error testing database connection', err);

        let errorMessage = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: 'danger',
            message: errorMessage
          }
        });
      });
  }

  renderUsername() {
    return (
      <div className="form-group row">
        <label className={LABEL_COL_CLASS}>
          {T.translate(`${PREFIX}.username`)}
        </label>
        <div className={INPUT_COL_CLASS}>
          <input
            type="text"
            className="form-control"
            value={this.state.username}
            onChange={this.handleChange.bind(this, 'username')}
          />
        </div>
      </div>
    );
  }

  renderPassword() {
    return (
      <div className="form-group row">
        <label className={LABEL_COL_CLASS}>
          {T.translate(`${PREFIX}.password`)}
        </label>
        <div className={INPUT_COL_CLASS}>
          <input
            type="password"
            className="form-control"
            value={this.state.password}
            onChange={this.handleChange.bind(this, 'password')}
          />
        </div>
      </div>
    );
  }

  renderTestButton() {
    return (
      <div className="form-group row">
        <div className="col-xs-8 offset-xs-4">
          <button
            className="btn btn-secondary"
            onClick={this.testConnection}
          >
            Test Connection
          </button>

          {
            this.state.connectionResult ?
              (
                <span
                  className={`connection-check text-${this.state.connectionResult.type}`}
                >
                  {this.state.connectionResult.message}
                </span>
              )
            :
              null
          }
        </div>
      </div>
    );
  }

  renderBasic() {
    return (
      <div>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.hostname`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <input
              type="text"
              className="form-control"
              value={this.state.hostname}
              onChange={this.handleChange.bind(this, 'hostname')}
            />
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.port`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <input
              type="text"
              className="form-control"
              value={this.state.port}
              onChange={this.handleChange.bind(this, 'port')}
            />
          </div>
        </div>

        {this.renderUsername()}

        {this.renderPassword()}

        {this.renderTestButton()}

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.database`)}
          </label>
          <div className={INPUT_COL_CLASS}>
            <input
              type="text"
              className="form-control"
              value={this.state.database}
              onChange={this.handleChange.bind(this, 'database')}
            />
          </div>
        </div>
      </div>

    );
  }

  renderAdvanced() {
    return (
      <div>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.connectionString`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <input
              type="text"
              className="form-control"
              value={this.state.connectionString}
              onChange={this.handleChange.bind(this, 'connectionString')}
            />
          </div>
        </div>

        {this.renderUsername()}

        {this.renderPassword()}
      </div>
    );
  }

  renderDriverInfo() {
    let db = this.props.db;

    return (
      <div className="row driver-info">
        <div className="col-xs-4 text-xs-right">
          <div className={`db-image db-${db.name}`}></div>
          <span>{db.label}</span>
        </div>

        <div className="col-xs-8 driver-detail">
          <span>
            {db.pluginInfo.version}
          </span>

          <span>
            <span className="fa fa-fw">
              <IconSVG name="icon-check-circle" />
            </span>

            <span>{T.translate(`${PREFIX}.driverInstalled`)}</span>
          </span>
        </div>
      </div>
    );
  }

  renderConnectionInfo() {
    if (this.state.connType === CONN_TYPE.basic) {
      return this.renderBasic();
    }

    return this.renderAdvanced();
  }

  renderAddConnectionButton() {
    let disabled = !this.state.name;

    if (this.state.connType === CONN_TYPE.basic) {
      disabled = disabled || !this.state.hostname || !this.state.port;
    } else {
      disabled = disabled || !this.state.connectionString;
    }

    let onClickFn = this.addConnection;

    if (this.props.mode === 'EDIT') {
      onClickFn = this.editConnection;
    }

    return (
      <div className="row">
        <div className="col-xs-8 offset-xs-4">
          <button
            className="btn btn-primary"
            onClick={onClickFn}
            disabled={disabled}
          >
            {T.translate(`${PREFIX}.Buttons.${this.props.mode}`)}
          </button>
        </div>
      </div>
    );
  }

  render() {
    let backlink = (
      <div className="back-link-container">
        <span
          className="back-link"
          onClick={this.props.back}
        >
          <span className="fa fa-fw">
            <IconSVG name="icon-angle-double-left" />
          </span>
          <span>{T.translate(`${PREFIX}.backButton`)}</span>
        </span>
      </div>
    );

    if (this.props.mode !== 'ADD') {
      backlink = null;
    }

    return (
      <div className="database-detail">
        <div>
          {this.renderDriverInfo()}

          <div className="row">
            <div className="col-xs-12 text-xs-right">
              <span className="asterisk">*</span>
              <em>{T.translate(`${PREFIX}.required`)}</em>
            </div>
          </div>

          <form onSubmit={this.preventDefault}>
            <div className="form-group row">
              <label className={LABEL_COL_CLASS}>
                {T.translate(`${PREFIX}.name`)}
                <span className="asterisk">*</span>
              </label>
              <div className={INPUT_COL_CLASS}>
                <input
                  type="text"
                  className="form-control"
                  value={this.state.name}
                  onChange={this.handleChange.bind(this, 'name')}
                />
              </div>
            </div>

            <div className="form-group row">
              <label className={LABEL_COL_CLASS}>
                {T.translate(`${PREFIX}.connType`)}
              </label>
              <div className={`${INPUT_COL_CLASS} connection-type`}>
                <span
                  onClick={this.handleConnTypeChange.bind(this, CONN_TYPE.basic)}
                  className={classnames({'active': this.state.connType === CONN_TYPE.basic})}
                >
                  {T.translate(`${PREFIX}.basic`)}
                </span>
                <span className="divider">|</span>
                <span
                  onClick={this.handleConnTypeChange.bind(this, CONN_TYPE.advanced)}
                  className={classnames({'active': this.state.connType === CONN_TYPE.advanced})}
                >
                  {T.translate(`${PREFIX}.advanced`)}
                </span>
              </div>
            </div>

            {this.renderConnectionInfo()}

          </form>

          {this.renderAddConnectionButton()}
        </div>

        {backlink}
      </div>
    );
  }
}

DatabaseDetail.propTypes = {
  back: PropTypes.func,
  db: PropTypes.object,
  onAdd: PropTypes.func,
  mode: PropTypes.oneOf(['ADD', 'EDIT', 'DUPLICATE']).isRequired,
  connectionId: PropTypes.string,
  connInfo: PropTypes.object
};

