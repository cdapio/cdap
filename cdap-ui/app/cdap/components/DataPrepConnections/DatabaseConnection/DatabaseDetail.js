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
      host: '',
      port: '',
      username: '',
      password: '',
      database: '',
      jdbcString: ''
    };

    this.addConnection = this.addConnection.bind(this);
  }

  handleChange(key, e) {
    this.setState({[key]: e.target.value});
  }

  handleConnTypeChange(type) {
    this.setState({connType: type});
  }

  addConnection() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let properties;

    if (this.state.connType === CONN_TYPE.basic) {
      properties = {
        hostname: this.state.host,
        port: this.state.port,
        userName: this.state.username,
        password: this.state.password,
        database: this.state.database
      };
    } else {
      properties = {
        connectionString: this.state.jdbcString,
        userName: this.state.username,
        password: this.state.password
      };
    }

    let requestBody = {
      name: this.state.name,
      type: 'DATABASE',
      properties
    };

    MyDataPrepApi.createConnection({namespace}, requestBody)
      .subscribe(() => {
        this.props.onAdd();
      }, (err) => {
        console.log('err', err);
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

  renderBasic() {
    return (
      <div>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.host`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <input
              type="text"
              className="form-control"
              value={this.state.host}
              onChange={this.handleChange.bind(this, 'host')}
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
              value={this.state.jdbcString}
              onChange={this.handleChange.bind(this, 'jdbcString')}
            />
          </div>
        </div>

        {this.renderUsername()}

        {this.renderPassword()}
      </div>
    );
  }

  renderDriverInfo() {
    return (
      <div className="row driver-info">
        <div className="col-xs-4 text-xs-right">
          <div className={`db-image ${this.props.db.database.classname}`}></div>
          <span>{this.props.db.database.name}</span>
        </div>

        <div className="col-xs-8 driver-detail">
          <span>
            {this.props.db.pluginInfo.artifactVersion}
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
      disabled = disabled || !this.state.host || !this.state.port;
    } else {
      disabled = disabled || !this.state.jdbcString;
    }

    return (
      <div className="row">
        <div className="col-xs-8 offset-xs-4">
          <button
            className="btn btn-primary"
            onClick={this.addConnection}
            disabled={disabled}
          >
            {T.translate(`${PREFIX}.addConnectionButton`)}
          </button>
        </div>
      </div>
    );
  }

  render() {
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

          <form>
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
      </div>
    );
  }
}

DatabaseDetail.propTypes = {
  back: PropTypes.func,
  db: PropTypes.object,
  onAdd: PropTypes.func
};

