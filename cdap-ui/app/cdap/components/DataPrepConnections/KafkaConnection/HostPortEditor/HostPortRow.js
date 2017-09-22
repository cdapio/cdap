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

export default class HostPortRow extends Component {
  constructor(props) {
    super(props);

    this.keyDown = this.keyDown.bind(this);
  }

  keyDown(e) {
    if (e.keyCode === 13) {
      this.props.addRow();
    }
  }

  render() {
    return (
      <div className="host-port-row-container">
        <div className="host-input-container">
          <input
            type="text"
            value={this.props.host}
            autoFocus
            onKeyDown={this.keyDown}
            onChange={this.props.onChange.bind(null, 'host')}
            className="form-control"
          />
        </div>

        <span className="port-label">
          Port:
        </span>
        <div className="port-input-container">
          <input
            type="number"
            value={this.props.port}
            onKeyDown={this.keyDown}
            onChange={this.props.onChange.bind(null, 'port')}
            className="form-control"
          />
        </div>

        <div className="action-buttons-container text-xs-right">
          <button
            className="btn add-row-btn btn-link"
            onClick={this.props.addRow}
          >
            <i className="fa fa-plus" />
          </button>
          <button
            className="btn remove-row-btn btn-link"
            onClick={this.props.removeRow}
          >
            <i className="fa fa-trash text-danger" />
          </button>
        </div>
      </div>
    );
  }
}

HostPortRow.propTypes = {
  className: PropTypes.string,
  host: PropTypes.string,
  port: PropTypes.string,
  index: PropTypes.number,
  onChange: PropTypes.func,
  addRow: PropTypes.func,
  removeRow: PropTypes.func,
};
