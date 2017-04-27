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
import classnames from 'classnames';

export default class ColumnsTabRow extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selected: props.selected
    };

    this.toggleRowSelect = this.toggleRowSelect.bind(this);
  }

  toggleRowSelect() {
    let newState = !this.state.selected;
    let elem = document.getElementById(`column-${this.props.columnName}`);
    if (newState) {
      elem.scrollIntoView();
    }

    this.setState({selected: newState});
    this.props.setSelect(this.props.columnName, newState);
  }

  renderTypesTable() {
    let types = this.props.rowInfo.types;
    if (!types) { return; }

    let headers = Object.keys(types);

    return (
      <div className="types-table-container">
        <table className="table">
          <thead>
            <tr>
              <th>Inferred Type</th>
              <th>% Chance</th>
            </tr>
          </thead>

          <tbody>
            {
              headers.map((head) => {
                return (
                  <tr key={head}>
                    <td>{head}</td>
                    <td>{types[head]}</td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }

  renderSelected() {
    if (!this.state.selected) { return null; }

    let generalInfo = this.props.rowInfo.general;

    let nonNull = generalInfo['non-null'] || 0,
        nullCell = generalInfo['null'] || 0,
        empty = generalInfo['empty'] || 0;

    let filled = nonNull - empty;

    return (
      <div className="selected-row">
        <div className="quality-bar">
          <span
            className="filled"
            style={{width: `${filled}%`}}
          />

          <span
            className="empty"
            style={{width: `${empty}%`}}
          />

          <span
            className="null-cell"
            style={{width: `${nullCell}%`}}
          />
        </div>

        {this.renderTypesTable()}

      </div>
    );
  }

  render() {
    return (
      <div
        className="columns-tab-row"
        id={`columns-tab-row-${this.props.columnName}`}
      >
        <div
          className={classnames('row-header', {
            'selected': this.state.selected,
            'invalid': !this.props.rowInfo.isValid
          })}
        >
          <span
            onClick={this.toggleRowSelect}
            className={classnames('fa row-header-checkbox', {
              'fa-square-o': !this.state.selected,
              'fa-check-square': this.state.selected
            })}
          />
          <span>
            {this.props.index + 1}. {this.props.columnName}
          </span>
        </div>

        {this.renderSelected()}
      </div>
    );
  }
}

ColumnsTabRow.propTypes = {
  rowInfo: PropTypes.object,
  index: PropTypes.number,
  columnName: PropTypes.string,
  selected: PropTypes.bool,
  setSelect: PropTypes.func
};
