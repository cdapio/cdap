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
import Histogram from 'components/Wrangler/Histogram';
import {createBucket} from 'components/Wrangler/data-buckets';
import {
  dropColumn,
  renameColumn,
  splitColumn,
  mergeColumn,
  uppercaseColumn,
  lowercaseColumn,
  titlecaseColumn,
  substringColumn
} from 'components/Wrangler/column-transforms';

export default class WrangleData extends Component {
  constructor(props) {
    super(props);

    let headers = Object.keys(this.props.data[0]);

    let columnTypes = {};
    let histogram = {};
    headers.forEach((column) => {
      let columnType = inferColumn(this.props.data, column);
      columnTypes[column] = columnType;

      histogram[column] = createBucket(this.props.data, column, columnType);
    });

    this.state = {
      headersList: headers,
      data: this.props.data,
      history: [],
      histogram: histogram,
      columnTypes: columnTypes,
      activeSelection: null,
      activeSelectionType: null,
      isRename: false,
      isSplit: false,
      isMerge: false,
      isSubstring: false,
    };

    this.dropColumn = this.dropColumn.bind(this);
    this.splitColumnClick = this.splitColumnClick.bind(this);
    this.mergeColumnClick = this.mergeColumnClick.bind(this);
    this.renameColumnClick = this.renameColumnClick.bind(this);
    this.onRename = this.onRename.bind(this);
    this.onSplit = this.onSplit.bind(this);
    this.onMerge = this.onMerge.bind(this);
    this.substringColumnClick = this.substringColumnClick.bind(this);
    this.onSubstring = this.onSubstring.bind(this);
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

        <button
          className="btn btn-default"
          onClick={this.transformCase.bind(this, 'UPPERCASE')}
        >
          UPPER CASE
        </button>

        <button
          className="btn btn-default"
          onClick={this.transformCase.bind(this, 'LOWERCASE')}
        >
          lower case
        </button>

        <button
          className="btn btn-default"
          onClick={this.transformCase.bind(this, 'TITLECASE')}
        >
          Title Case
        </button>

        <button
          className="btn btn-default"
          onClick={this.substringColumnClick}
        >
          Substring
        </button>
        {this.renderSubstring()}
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

  renderSubstring() {
    if (!this.state.isSubstring) { return null; }

    return (
      <div className="substring-input">
        <div>
          <label className="control-label">Begin Index:</label>
          <input
            type="number"
            className="form-control"
            onChange={e => this.setState({substringBeginIndex: e.target.value})}
          />
        </div>

        <div>
          <label className="control-label">End Index:</label>
          <input
            type="number"
            className="form-control"
            onChange={e => this.setState({substringEndIndex: e.target.value})}
          />
        </div>

        <div>
          <label className="control-label">New Column Name:</label>
          <input
            type="text"
            className="form-control"
            onChange={e => this.setState({substringColumnName: e.target.value})}
          />
        </div>
        <button
          className="btn btn-success"
          onClick={this.onSubstring}
        >
          Save
        </button>
      </div>
    );
  }

  addColumnMetadata(columns, index, data) {
    let headersList = this.state.headersList;
    let columnTypes = this.state.columnTypes;
    let histogram = this.state.histogram;

    columns.forEach((column, i) => {
      headersList.splice(index+i, 0, column);

      let columnType = inferColumn(data, column);
      columnTypes[column] = columnType;
      histogram[column] = createBucket(data, column, columnType);
    });

    return {
      headersList,
      columnTypes,
      histogram
    };
  }

  removeColumnMetadata(columns) {
    let headersList = this.state.headersList;
    let columnTypes = this.state.columnTypes;
    let histogram = this.state.histogram;

    columns.forEach((column) => {
      headersList.splice(headersList.indexOf(column), 1);
      delete columnTypes[column];
      delete histogram[column];
    });

    return {
      headersList,
      columnTypes,
      histogram
    };
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

    let {
      headersList,
      columnTypes,
      histogram
    } = this.removeColumnMetadata([columnToDrop]);

    this.setState({
      activeSelection: null,
      activeSelectionType: null,
      data: formattedData,
      history,
      headersList,
      columnTypes,
      histogram
    });
  }


  // RENAME
  renameColumnClick() {
    this.setState({
      isMerge: false,
      isSplit: false,
      isRename: true,
      isSubstring: false
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
      isRename: false,
      isSubstring: false
    });
  }

  onSplit() {
    const delimiter = this.splitDelimiter.value;
    const firstSplit = this.firstSplit.value;
    const secondSplit = this.secondSplit.value;
    const columnToSplit = this.state.activeSelection;

    let formattedData = splitColumn(this.state.data, delimiter, columnToSplit, firstSplit, secondSplit);

    const index = this.state.headersList.indexOf(columnToSplit);
    let {
      headersList,
      columnTypes,
      histogram
    } = this.addColumnMetadata([firstSplit, secondSplit], index+1, formattedData);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'SPLIT',
      payload: [columnToSplit]
    });

    this.setState({
      isSplit: false,
      data: formattedData,
      history,
      headersList,
      columnTypes,
      histogram
    });
  }

  // MERGE
  mergeColumnClick() {
    this.setState({
      isMerge: true,
      isSplit: false,
      isRename: false,
      isSubstring: false
    });
  }

  onMerge() {
    const mergeWith = this.mergeWith.value;
    const columnToMerge = this.state.activeSelection;
    const joinBy = this.joinBy.value;
    const columnName = this.mergedColumnName.value;

    let formattedData = mergeColumn(this.state.data, joinBy, columnToMerge, mergeWith, columnName);

    const index = this.state.headersList.indexOf(columnToMerge);
    let {
      headersList,
      columnTypes,
      histogram
    } = this.addColumnMetadata([columnName], index+1, formattedData);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'MERGE',
      payload: [columnToMerge, mergeWith]
    });

    this.setState({
      isMerge: false,
      data: formattedData,
      history,
      headersList,
      columnTypes,
      histogram
    });
  }

  // CASE TRANSFORMATIONS
  transformCase(type) {
    const columnToTransform = this.state.activeSelection;

    let formattedData;
    switch (type) {
      case 'UPPERCASE':
        formattedData = uppercaseColumn(this.state.data, columnToTransform);
        break;
      case 'LOWERCASE':
        formattedData = lowercaseColumn(this.state.data, columnToTransform);
        break;
      case 'TITLECASE':
        formattedData = titlecaseColumn(this.state.data, columnToTransform);
        break;
    }

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: type,
      payload: [columnToTransform]
    });

    this.setState({
      data: formattedData,
      history: history
    });
  }

  // SUBSTRING
  substringColumnClick() {
    this.setState({
      isRename: false,
      isSplit: false,
      isMerge: false,
      isSubstring: true
    });
  }

  onSubstring() {
    const columnToSub = this.state.activeSelection;
    const beginIndex = this.state.substringBeginIndex;
    const endIndex = this.state.substringEndIndex;
    const substringColumnName = this.state.substringColumnName;

    let formattedData = substringColumn(this.state.data, columnToSub, beginIndex, endIndex, substringColumnName);

    const index = this.state.headersList.indexOf(columnToSub);
    let {
      headersList,
      columnTypes,
      histogram
    } = this.addColumnMetadata([substringColumnName], index+1, formattedData);

    let history = this.state.history;
    history.push({
      id: shortid.generate(),
      action: 'SUBSTRING',
      payload: [columnToSub]
    });

    this.setState({
      isSubstring: false,
      data: formattedData,
      history,
      headersList,
      columnTypes,
      histogram
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
              <tr>
                <th></th>
                {
                  headers.map((head) => {
                    return (
                      <th key={head}>
                        <Histogram
                          data={this.state.histogram[head].data}
                          labels={this.state.histogram[head].labels}
                        />
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
