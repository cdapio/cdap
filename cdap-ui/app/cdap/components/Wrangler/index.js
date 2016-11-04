/*
 * Copyright © 2016 Cask Data, Inc.
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
import Papa from 'papaparse';
import shortid from 'shortid';

require('./Wrangler.less');

export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      header: false,
      dynamicTyping: false,
      skipEmptyLines: false,
      data: []
    };
  }

  wrangle() {
    let input = this.wranglerInput.value;

    let papaConfig = {
      header: this.state.header,
      dynamicTyping: this.state.dynamicTyping,
      skipEmptyLines: this.state.skipEmptyLines,
      complete: this.handleData.bind(this)
    };

    Papa.parse(input, papaConfig);
  }

  handleData(papa) {
    let formattedData;
    if (Array.isArray(papa.data[0])) {
      formattedData = papa.data.map((row) => {
        let obj = {};

        row.forEach((col, index) => {
          let key = `column${index+1}`;
          obj[key] = col;
        });

        return obj;
      });
    } else {
      formattedData = papa.data;
    }

    this.setState({
      data: formattedData
    });
  }

  handleSetHeaders() {
    this.setState({header: !this.state.header});
  }
  handleSetDynamicTyping() {
    this.setState({dynamicTyping: !this.state.dynamicTyping});
  }
  handleSetSkipEmptyLines() {
    this.setState({skipEmptyLines: !this.state.skipEmptyLines});
  }

  renderResultTable() {
    if (this.state.data.length === 0) { return null; }

    const data = this.state.data;

    let headers = Object.keys(data[0]);

    return (
      <div className="wrangler-data">
        <table className="table">
          <thead>
            <tr>
              { headers.map((head) => <th key={head}>{head}</th>) }
            </tr>
          </thead>

          <tbody>
            { data.map((row) => {
              return (
                <tr key={shortid.generate()}>
                  { headers.map((head) => <td key={shortid.generate()}>{row[head]}</td>) }
                </tr>
              );
            }) }
          </tbody>
        </table>
      </div>
    );
  }

  render() {
    return (
      <div className="wrangler-container">
        <h1>Wrangler</h1>

        <div className="wrangler-input row">
          <div className="col-xs-6">
            <h3>Upload File</h3>

            <input type="file" />
          </div>
          <div className="col-xs-6">
            <h3>Copy Input Text</h3>
            <textarea
              className="form-control"
              ref={(ref) => this.wranglerInput = ref}
            />
          </div>
        </div>

        <div className="parse-options">
          <h3>Options</h3>

          <form className="form-inline">
            <div className="delimiter">
              {/* delimiter */}
              <label className="control-label">Delimiter</label>
              <input type="text" className="form-control" />
            </div>

            <div className="checkbox">
              {/* header */}
              <label>
                <input type="checkbox"
                  onChange={this.handleSetHeaders.bind(this)}
                  checked={this.state.headers}
                /> First line as column name?
              </label>
            </div>

            <div className="checkbox">
              {/* skipEmptyLines */}
              <label>
                <input type="checkbox"
                  onChange={this.handleSetSkipEmptyLines.bind(this)}
                /> Skip empty lines?
              </label>
            </div>

            <div className="checkbox">
              {/* dynamicTyping */}
              <label>
                <input type="checkbox"
                  onChange={this.handleSetDynamicTyping.bind(this)}
                /> Attempt to convert number and booleans?
              </label>
            </div>
          </form>
        </div>

        <br/>
        <div className="text-center">
          <button
            className="btn btn-primary"
            onClick={this.wrangle.bind(this)}
          >
            Wrangle
          </button>
        </div>

        <br/>

        {this.renderResultTable()}

      </div>
    );
  }
}
