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

import React, { Component, PropTypes } from 'react';
import WrangleHistory from 'components/Wrangler/WrangleHistory';
import {inferColumn} from 'components/Wrangler/type-inference';
import classnames from 'classnames';
import shortid from 'shortid';
import {
  dropColumn,
  renameColumn,
  splitColumn,
  mergeColumn
} from 'components/Wrangler/column-transforms';

export default class WrangleData extends Component {
  constructor(props) {
    super(props);

    let headers = Object.keys(this.props.data[0]);

    let columnTypes = {};
    headers.forEach((column) => {
      columnTypes[column] = inferColumn(this.props.data, column);
    });

    this.state = {
      headersList: headers,
      data: this.props.data,
      history: [],
      columnTypes: columnTypes,
      activeSelection: null,
      activeSelectionType: null,
      isRename: false,
      isSplit: false,
      isMerge: false
    };

    this.dropColumn = this.dropColumn.bind(this);
    this.splitColumnClick = this.splitColumnClick.bind(this);
    this.mergeColumnClick = this.mergeColumnClick.bind(this);
    this.renameColumnClick = this.renameColumnClick.bind(this);
    this.onRename = this.onRename.bind(this);
    this.onSplit = this.onSplit.bind(this);
    this.onMerge = this.onMerge.bind(this);
  }

  renderActionList() {
    if (this.state.activeSelectionType === 'COLUMN') {
      return this.renderColumnActions();
    } else {
      return null;
    }
  }

  renderColumnActions() {
    return (
      <div className="btn-group-vertical">
        <button
          className="btn btn-default"
          onClick={this.dropColumn}
        >
          Drop column
        </button>
        <button
          className="btn btn-default"
          onClick={this.splitColumnClick}
        >
          Split column
        </button>
        {this.renderSplit()}

        <button
          className="btn btn-default"
          onClick={this.mergeColumnClick}
        >
          Merge column
        </button>
        {this.renderMerge()}

        <button
          className="btn btn-default"
          onClick={this.renameColumnClick}
        >
          Rename column
        </button>
        {this.renderRename()}
      </div>
    );
  }

  columnClickHandler(column) {
    this.setState({
      activeSelectionType: 'COLUMN',
      activeSelection: column
    });
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
          onClick={this.onRename}
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
          onClick={this.onSplit}
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
          onClick={this.onMerge}
        >
          Save
        </button>
      </div>
    );
  }


  // DROP COLUMN
  dropColumn() {
    const columnToDrop = this.state.activeSelection;

    let formattedData = dropColumn(this.state.data, columnToDrop);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'DROP COLUMN',
      payload: [columnToDrop]
    });

    let headers = this.state.headersList;
    headers.splice(headers.indexOf(columnToDrop), 1);

    let columnTypes = this.state.columnTypes;
    delete columnTypes[columnToDrop];

    this.setState({
      activeSelection: null,
      activeSelectionType: null,
      headersList: headers,
      columnTypes: columnTypes,
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

    let formattedData = renameColumn(this.state.data, originalName, newName);

    let headers = this.state.headersList;
    headers[headers.indexOf(originalName)] = newName;

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'RENAME',
      payload: [originalName, newName]
    });

    let columnTypes = this.state.columnTypes;
    columnTypes[newName] = columnTypes[originalName];
    delete columnTypes[originalName];

    this.setState({
      headersList: headers,
      columnTypes: columnTypes,
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

    let formattedData = splitColumn(this.state.data, delimiter, columnToSplit, firstSplit, secondSplit);

    let headers = this.state.headersList;
    let index = headers.indexOf(columnToSplit);
    headers.splice(index+1, 0, firstSplit, secondSplit);

    let columnTypes = this.state.columnTypes;
    columnTypes[firstSplit] = inferColumn(formattedData, firstSplit);
    columnTypes[secondSplit] = inferColumn(formattedData, secondSplit);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'SPLIT',
      payload: [columnToSplit]
    });

    this.setState({
      isSplit: false,
      headersList: headers,
      columnTypes: columnTypes,
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

    let formattedData = mergeColumn(this.state.data, joinBy, columnToMerge, mergeWith, columnName);

    let headers = this.state.headersList;
    const index = headers.indexOf(columnToMerge);
    headers.splice(index+1, 0, columnName);

    let columnTypes = this.state.columnTypes;
    columnTypes[columnName] = inferColumn(formattedData, columnName);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'MERGE',
      payload: [columnToMerge, mergeWith]
    });

    this.setState({
      isMerge: false,
      headersList: headers,
      columnTypes: columnTypes,
      data: formattedData,
      history: history
    });
  }

  render() {

    const headers = this.state.headersList;
    const data = this.state.data;

    return (
      <div className="wrangler-data row">
        <div className="col-xs-3 wrangle-transforms">
          <h4>Actions</h4>

          {this.renderActionList()}

          <hr/>
          <h4>History</h4>

          <WrangleHistory
            historyArray={this.state.history}
          />

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
                        {head} ({this.state.columnTypes[head]})
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
}

WrangleData.defaultProps = {
  data: []
};

WrangleData.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object)
};
