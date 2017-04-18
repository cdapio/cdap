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

import React, { Component } from 'react';
import {MyBlankpathApi} from 'api/blankpath';

require('./HTTPExecutor.scss');

const HTTP_METHODS = [
  'GET',
  'POST',
  'PUT',
  'DELETE'
];

export default class HTTPExecutor extends Component {
  constructor(props) {
    super(props);

    this.state = {
      method: 'GET',
      path: '',
      body: '',
      data: null
    };

    this.onMethodChange = this.onMethodChange.bind(this);
    this.onPathChange = this.onPathChange.bind(this);
    this.onBodyChange = this.onBodyChange.bind(this);
    this.tidy = this.tidy.bind(this);
    this.send = this.send.bind(this);
  }

  onPathChange(e) {
    this.setState({path: e.target.value});
  }

  onMethodChange(e) {
    this.setState({method: e.target.value});
  }

  onBodyChange(e) {
    this.setState({body: e.target.value});
  }

  tidy() {
    try {
      let body = JSON.parse(this.state.body);

      this.setState({body: JSON.stringify(body, null, 4)});
    } catch (e) {
      return;
    }
  }

  send() {
    let params = {
      path: this.state.path
    };

    let body;
    if (this.state.method !== 'GET') {
      body = this.formatBody();
    }

    let api;
    switch (this.state.method) {
      case 'GET':
        api = MyBlankpathApi.get(params);
        break;
      case 'POST':
        api = MyBlankpathApi.post(params, body);
        break;
      case 'PUT':
        api = MyBlankpathApi.put(params, body);
        break;
      case 'DELETE':
        api = MyBlankpathApi.delete(params, body);
        break;
    }

    api.subscribe((res) => {
      this.setState({data: res});
    }, (err) => {
      this.setState({data: err});
    });
  }

  formatBody() {
    if (!this.state.body) { return null;}

    let body = this.state.body;

    try {
      body = JSON.parse(body);
    } catch (e) {
      return body;
    }

    return body;
  }

  renderSelect() {
    return (
      <span className="input-group-addon select-method">
        <select
          className="form-control"
          onChange={this.onMethodChange}
          value={this.state.method}
        >
          {
            HTTP_METHODS.map((method) => {
              return (
                <option
                  className="dropdown-item"
                  key={method}
                >
                  {method}
                </option>
              );
            })
          }
        </select>
      </span>
    );
  }

  renderBody() {
    if (this.state.method === 'GET') { return null; }

    return (
      <div className="request-body-container">
        <h5>
          Request Body
        </h5>

        <div className="body-input">
          <textarea
            className="form-control"
            onChange={this.onBodyChange}
            value={this.state.body}
          >
          </textarea>

          <button
            className="btn btn-secondary tidy-button"
            onClick={this.tidy}
          >
            Tidy
          </button>
        </div>
      </div>
    );
  }

  renderData() {
    if (this.state.data === null) { return; }

    console.log('data', this.state.data);

    let displayData = JSON.stringify(this.state.data, null, 4);

    return (
      <div className="response-data-container">
        <pre>{displayData}</pre>
      </div>
    );
  }

  renderInput() {
    return (
      <div className="input-group">
        {this.renderSelect()}
        <span className="input-group-addon">
          localhost:11015/v3/
        </span>
        <input
          type="text"
          className="form-control"
          onChange={this.onPathChange}
          value={this.state.path}
          placeholder="path"
        />
      </div>
    );
  }

  render() {
    return (
      <div className="http-executor-container">
        <h3>HTTP Executor</h3>

        <div className="clearfix">
          <div className="request-container float-xs-left">
            {this.renderInput()}
          </div>

          <div className="request-button float-xs-left">
            <button
              className="btn btn-primary"
              onClick={this.send}
            >
              SEND
            </button>
          </div>
        </div>

        {this.renderBody()}
        <hr />
        {this.renderData()}
      </div>
    );
  }
}
