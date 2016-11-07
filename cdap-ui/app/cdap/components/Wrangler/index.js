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
import classnames from 'classnames';

require('./Wrangler.less');

export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      header: false,
      dynamicTyping: false,
      skipEmptyLines: false,
      originalData: [],
      headersList: [],
      data: [],
      history: [],
      activeSelection: null,
      activeSelectionType: null,
      isRename: false,
      isSplit: false,
      isMerge: false
    };
  }

  wrangle() {
    // let input = this.wranglerInput.value;

    let input = 'col1 hehe,col2,col3\nedwin elia,1,true\nelia edwin,2,3';

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

    let headers = Object.keys(formattedData[0]);

    this.setState({
      headersList: headers,
      originalData: formattedData,
      data: formattedData,
      history: [],
      activeSelectionType: null,
      activeSelection: null
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

  columnClickHandler(column) {
    console.log('column', column);
    this.setState({
      activeSelectionType: 'COLUMN',
      activeSelection: column
    });
  }

  renderActionList() {
    if (this.state.activeSelectionType === 'COLUMN') {
      return this.renderColumnActions();
    }
  }

  renderRename() {
    if (!this.state.isRename) { return null; }

    return (
      <div className="rename-input">
        <label className="label-control">New name</label>
        <input
          type="text"
          className="form-control"
          ref={(ref) => this.renameInput = ref}
        />
        <button
          className="btn btn-success"
          onClick={this.onRename.bind(this)}
        >
          Save
        </button>
      </div>
    );
  }

  renderSplit() {
    if (!this.state.isSplit) { return null; }

    return (
      <div className="split-input">
        <div>
          <label className="control-label">Split by first occurence of:</label>
          <input
            type="text"
            className="form-control"
            ref={(ref) => this.splitDelimiter = ref}
          />
        </div>
        <div>
          <label className="control-label">First Split Name:</label>
          <input
            type="text"
            className="form-control"
            ref={(ref) => this.firstSplit = ref}
          />
        </div>
        <div>
          <label className="control-label">Second Split Name:</label>
          <input
            type="text"
            className="form-control"
            ref={(ref) => this.secondSplit = ref}
          />
        </div>
        <button
          className="btn btn-success"
          onClick={this.onSplit.bind(this)}
        >
          Save
        </button>
      </div>
    );
  }

  renderMerge() {
    if (!this.state.isMerge) { return null; }

    let headers = Object.keys(this.state.data[0]);

    // remove currently selected from list of columns
    headers.splice(headers.indexOf(this.state.activeSelection), 1);

    return (
      <div className="merge-input">
        <div>
          <label className="control-label">Merge with</label>
          <select
            defaultValue={headers[0]}
            ref={(ref) => this.mergeWith = ref}
          >
            {
              headers.map((header) => {
                return (
                  <option
                    value={header}
                    key={header}
                  >
                    {header}
                  </option>
                );
              })
            }
          </select>
        </div>
        <div>
          <label className="control-label">Join by:</label>
          <input
            type="text"
            className="form-control"
            ref={(ref) => this.joinBy = ref}
          />
        </div>
        <div>
          <label className="control-label">Merged Column Name:</label>
          <input
            type="text"
            className="form-control"
            ref={(ref) => this.mergedColumnName = ref}
          />
        </div>
        <button
          className="btn btn-success"
          onClick={this.onMerge.bind(this)}
        >
          Save
        </button>
      </div>
    );
  }

  renderColumnActions() {
    return (
      <div className="btn-group-vertical">
        <button
          className="btn btn-default"
          onClick={this.dropColumn.bind(this)}
        >
          Drop column
        </button>
        <button
          className="btn btn-default"
          onClick={this.splitColumnClick.bind(this)}
        >
          Split column
        </button>
        {this.renderSplit()}

        <button
          className="btn btn-default"
          onClick={this.mergeColumnClick.bind(this)}
        >
          Merge column
        </button>
        {this.renderMerge()}

        <button
          className="btn btn-default"
          onClick={this.renameColumnClick.bind(this)}
        >
          Rename column
        </button>
        {this.renderRename()}
      </div>
    );
  }

  // DROP COLUMN

  dropColumn() {
    const columnToDrop = this.state.activeSelection;

    let formattedData = this.state.data.map((row) => {
      delete row[columnToDrop];
      return row;
    });

    let history = this.state.history;
    history.push({
      action: 'DROP COLUMN',
      payload: [columnToDrop]
    });

    let headers = Object.keys(formattedData[0]);

    this.setState({
      activeSelection: null,
      activeSelectionType: null,
      headersList: headers,
      data: formattedData,
      history: history
    });
  }


  // RENAME

  renameColumnClick() {
    this.setState({
      isMerge: false,
      isSplit: false,
      isRename: true
    });
  }

  onRename() {
    const originalName = this.state.activeSelection;
    const newName = this.renameInput.value;

    let formattedData = this.state.data.map((row) => {
      row[newName] = row[originalName];
      delete row[originalName];
      return row;
    });

    let headers = this.state.headersList;
    headers[headers.indexOf(originalName)] = newName;

    let history = this.state.history;
    history.push({
      action: 'RENAME',
      payload: [originalName, newName]
    });

    this.setState({
      headersList: headers,
      data: formattedData,
      isRename: false,
      activeSelection: newName,
      history: history
    });
  }

  // SPLIT

  splitColumnClick() {
    this.setState({
      isMerge: false,
      isSplit: true,
      isRename: false
    });
  }

  onSplit() {
    const delimiter = this.splitDelimiter.value;
    const firstSplit = this.firstSplit.value;
    const secondSplit = this.secondSplit.value;

    const columnToSplit = this.state.activeSelection;

    let formattedData = this.state.data.map((row) => {
      let split = row[columnToSplit];

      let index = split.indexOf(delimiter);

      row[firstSplit] = split.slice(0,index);
      row[secondSplit] = split.slice(index+1);

      return row;
    });

    let headers = this.state.headersList;
    let index = headers.indexOf(columnToSplit);
    headers.splice(index+1, 0, firstSplit);
    headers.splice(index+2, 0, secondSplit);

    let history = this.state.history;
    history.push({
      action: 'SPLIT',
      payload: [columnToSplit]
    });

    this.setState({
      isSplit: false,
      headersList: headers,
      data: formattedData,
      history: history,
    });
  }

  // MERGE

  mergeColumnClick() {
    this.setState({
      isMerge: true,
      isSplit: false,
      isRename: false
    });
  }

  onMerge() {
    const mergeWith = this.mergeWith.value;
    const columnToMerge = this.state.activeSelection;
    const joinBy = this.joinBy.value;
    const columnName = this.mergedColumnName.value;

    let formattedData = this.state.data.map((row) => {
      let merged = row[columnToMerge].concat(joinBy, row[mergeWith]);
      row[columnName] = merged;

      return row;
    });

    let headers = this.state.headersList;
    const index = headers.indexOf(columnToMerge);
    headers.splice(index+1, 0, columnName);

    let history = this.state.history;
    history.push({
      action: 'MERGE',
      payload: [columnToMerge, mergeWith]
    });

    this.setState({
      isMerge: false,
      headersList: headers,
      data: formattedData,
      history: history
    });
  }

  renderHistory() {
    return (
      <ul>
        {
          this.state.history.map((history) => {
            return (
              <li key={shortid.generate()}>{history.action}: {history.payload.join(', ')}</li>
            );
          })
        }
      </ul>
    );
  }


  renderResultTable() {
    if (this.state.data.length === 0) { return null; }

    const data = this.state.data;

    let headers = this.state.headersList;

    return (
      <div className="wrangler-data row">
        <div className="col-xs-3 wrangle-transforms">
          <h4>Actions</h4>

          {this.renderActionList()}

          <hr/>
          <h4>History</h4>

          {this.renderHistory()}

        </div>
        <div className="col-xs-9 wrangle-results">
          <table className="table">
            <thead>
              <tr>
                <th></th>
                {
                  headers.map((head) => {
                    return (
                      <th
                        key={head}
                        onClick={this.columnClickHandler.bind(this, head)}
                        className={classnames('column-name', {
                          active: this.state.activeSelection === head
                        })}
                      >
                        {head}
                      </th>
                    );
                  })
                }
              </tr>
            </thead>

            <tbody>
              { data.map((row, index) => {
                return (
                  <tr key={shortid.generate()}>
                    <td>{index+1}</td>
                    {
                      headers.map((head) => {
                        return (
                          <td
                            key={shortid.generate()}
                            className={classnames({
                              active: this.state.activeSelection === head
                            })}
                          >
                            {row[head]}
                          </td>
                        );
                      })
                    }
                  </tr>
                );
              }) }
            </tbody>
          </table>
        </div>
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
