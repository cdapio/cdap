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
import WrangleData from 'components/Wrangler/WrangleData';
require('./Wrangler.less');

/**
 * 3 Steps for any transforms:
 *    1. Format the data
 *    2. Handle the ordering of Headers
 *    3. Handle the inferred type of the column
 **/
export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      header: false,
      skipEmptyLines: false,
      delimiter: '',
      wranglerInput: '',
      originalData: []
    };

    this.handleSetHeaders = this.handleSetHeaders.bind(this);
    this.handleSetSkipEmptyLines = this.handleSetSkipEmptyLines.bind(this);
    this.setDelimiter = this.setDelimiter.bind(this);
    this.wrangle = this.wrangle.bind(this);
    this.handleData = this.handleData.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
  }

  wrangle() {
    let input = this.state.wranglerInput;

    let papaConfig = {
      header: this.state.header,
      skipEmptyLines: this.state.skipEmptyLines,
      complete: this.handleData
    };

    if (this.state.delimiter) {
      papaConfig.delimiter = this.state.delimiter;
    }

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
      originalData: formattedData,
    });
  }

  setDelimiter(e) {
    this.setState({delimiter: e.target.value});
  }

  handleSetHeaders() {
    this.setState({header: !this.state.header});
  }
  handleSetSkipEmptyLines() {
    this.setState({skipEmptyLines: !this.state.skipEmptyLines});
  }
  handleTextInput(e) {
    this.setState({wranglerInput: e.target.value});
  }

  render() {
    return (
      <div className="wrangler-container">
        <h1>Wrangler</h1>

        <div className="wrangler-input row">
          <div className="col-xs-6">
            <h3>Instruction to try out</h3>

            <ol>
              <li>Paste some input data to the input text area</li>
              <li>Specify delimiter and configure options</li>
              <li>Click <strong>Wrangle</strong></li>
              <li>Click on a column header and the list of actions will show up on the left</li>
              <li>Choose any action and play around!</li>
            </ol>
          </div>
          <div className="col-xs-6">
            <h3>Copy Input Text</h3>
            <textarea
              className="form-control"
              onChange={this.handleTextInput}
            />
          </div>
        </div>

        <div className="parse-options">
          <h3>Options</h3>

          <form className="form-inline">
            <div className="delimiter">
              {/* delimiter */}
              <label className="control-label">Delimiter</label>
              <input
                type="text"
                className="form-control"
                onChange={this.setDelimiter}
              />
            </div>

            <div className="checkbox">
              {/* header */}
              <label>
                <input type="checkbox"
                  onChange={this.handleSetHeaders}
                  checked={this.state.headers}
                /> First line as column name?
              </label>
            </div>

            <div className="checkbox">
              {/* skipEmptyLines */}
              <label>
                <input type="checkbox"
                  onChange={this.handleSetSkipEmptyLines}
                /> Skip empty lines?
              </label>
            </div>
          </form>
        </div>

        <br/>
        <div className="text-center">
          <button
            className="btn btn-primary"
            onClick={this.wrangle}
          >
            Wrangle
          </button>
        </div>

        <br/>

        {
          this.state.originalData.length ?
            <WrangleData data={this.state.originalData} />
          :
            null
        }

      </div>
    );
  }
}
