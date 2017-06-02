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
import {preventPropagation} from 'services/helpers';
import isEqual from 'lodash/isEqual';
import classnames from 'classnames';

export default class ColumnsTabRow extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selected: props.selected,
      rowInfo: props.rowInfo,
      columnName: props.columnName,
      showTypes: false
    };

    this.toggleRowSelect = this.toggleRowSelect.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    let selectedChange = nextProps.selected !== this.state.selected;
    let columnNameChange = nextProps.columnName !== this.state.columnName;
    let rowInfoChange = !isEqual(nextProps.rowInfo, this.state.rowInfo);
    if (selectedChange || columnNameChange || rowInfoChange) {
      this.setState({
        columnName: nextProps.columnName,
        rowInfo: nextProps.rowInfo,
        selected: nextProps.selected
      });
    }
  }

  toggleRowSelect(e) {
    let newState = !this.state.selected;
    let elem = document.getElementById(`column-${this.props.columnName}`);
    if (newState) {
      elem.scrollIntoView();
    }

    this.setState({selected: newState});
    preventPropagation(e);
    this.props.setSelect(this.props.columnName, newState);
  }

  render() {
    let rowInfo = this.props.rowInfo || {};
    let general = rowInfo.general || {};
    let {empty: empty=0, 'non-null': nonEmpty=100} = general;

    let nonNull = Math.ceil(nonEmpty - empty);
    return (
      <tr
        className={classnames({
          'selected': this.state.selected
        })}
        onClick={this.props.onShowDetails}
      >
        <td>
          <span
            onClick={this.toggleRowSelect}
            className={classnames('fa row-header-checkbox', {
              'fa-square-o': !this.state.selected,
              'fa-check-square': this.state.selected
            })}
          />
        </td>
        <td>
          <span>
            {this.props.index + 1}
          </span>
        </td>
        <td>
          {this.props.columnName}
        </td>
        <td>
          <span>
            {`${nonNull}%`}
          </span>
        </td>
      </tr>
    );
  }
}
ColumnsTabRow.propTypes = {
  rowInfo: PropTypes.object,
  onShowDetails: PropTypes.func,
  index: PropTypes.number,
  columnName: PropTypes.string,
  selected: PropTypes.bool,
  setSelect: PropTypes.func
};
