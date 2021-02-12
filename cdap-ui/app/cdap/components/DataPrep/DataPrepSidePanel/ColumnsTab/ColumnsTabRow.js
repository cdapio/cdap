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
import { preventPropagation } from 'services/helpers';
import isEqual from 'lodash/isEqual';
import classnames from 'classnames';
import { CSSTransition } from 'react-transition-group';

class ColumnsTabRow extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selected: props.selected,
      rowInfo: props.rowInfo,
      columnName: props.columnName,
      showTypes: false,
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
        selected: nextProps.selected,
      });
    }
  }

  toggleRowSelect(e) {
    let newState = !this.state.selected;
    let elem = document.getElementById(`column-${this.props.columnName}`);
    if (newState) {
      elem.scrollIntoView();
    }

    this.setState({ selected: newState });
    preventPropagation(e);
    this.props.setSelect(this.props.columnName, newState);
  }

  render() {
    const rowInfo = this.props.rowInfo || {};
    const general = rowInfo.general || {};
    const { empty: empty = 0, 'non-null': nonEmpty = 100 } = general;

    // Round number to next lowest .1%
    // Number.toFixed() can round up and leave .0 on integers
    const nonNull = Math.floor((nonEmpty - empty) * 10) / 10;

    return (
      <CSSTransition in={this.props.isNew} timeout={4000} classNames="is-new" appear>
        <tr
          className={classnames({
            selected: this.state.selected,
          })}
          onClick={this.props.onShowDetails}
          ref={this.props.innerRef}
        >
          <td />
          <td>
            <span
              onClick={this.toggleRowSelect}
              className={classnames('fa row-header-checkbox', {
                'fa-square-o': !this.state.selected,
                'fa-check-square': this.state.selected,
              })}
            />
          </td>
          <td>
            <span>{this.props.index + 1}</span>
          </td>
          <td className="column-name">{this.props.columnName}</td>
          <td className="text-right non-null">
            <span>{`${nonNull}%`}</span>
          </td>
          <td />
        </tr>
      </CSSTransition>
    );
  }
}
ColumnsTabRow.propTypes = {
  rowInfo: PropTypes.object,
  onShowDetails: PropTypes.func,
  index: PropTypes.number,
  columnName: PropTypes.string,
  selected: PropTypes.bool,
  setSelect: PropTypes.func,
  isNew: PropTypes.bool,
  innerRef: PropTypes.oneOfType([
    PropTypes.func,
    PropTypes.shape({ current: PropTypes.elementType }),
  ]),
};

export default React.forwardRef((props, ref) => {
  return <ColumnsTabRow {...props} innerRef={ref} />;
});
