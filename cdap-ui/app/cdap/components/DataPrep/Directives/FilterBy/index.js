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
import shortid from 'shortid';
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import uniq from 'lodash/uniq';
import T from 'i18n-react';

require('./FilterByDirective.scss');

const SUFFIX = 'features.DataPrep.Directives.FilterBy';

export default class FilterByDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedCondition: 'NONE',
      textFilter: '',
      filterValueSearch: '',
      valueSelections: {}
    };

    this.handleConditionSelect = this.handleConditionSelect.bind(this);
    this.handleTextFilterChange = this.handleTextFilterChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
    this.handleFilterSearchChange = this.handleFilterSearchChange.bind(this);

    this.conditionsOptions = [
      'EMPTY',
      'NOTEMPTY',
      'TEXTCONTAINS',
      'TEXTNOTCONTAIN',
      'TEXTSTARTSWITH',
      'TEXTENDSWITH',
      'TEXTEXACTLY'
    ];
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.isOpen && !this.valueList) {
      let data = DataPrepStore.getState().dataprep.data;
      data = data.map((row) => {
        return row[this.props.column];
      });
      this.valueList = uniq(data).map((value) => {
        return {
          value,
          id: shortid.generate()
        };
      });

      let valueSelections = {};

      this.valueList.forEach((row) => {
        valueSelections[row.value] = false;
      });

      this.setState({ valueSelections });
    }
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  handleConditionSelect(e) {
    this.setState({selectedCondition: e.target.value});
  }

  handleTextFilterChange(e) {
    this.setState({textFilter: e.target.value});
  }

  handleFilterSearchChange(e) {
    this.setState({filterValueSearch: e.target.value});
  }

  filterClearAll(valueList) {
    let newState = this.state.valueSelections;

    valueList.forEach((row) => {
      newState[row.value] = false;
    });

    this.setState({valueSelections: newState});
  }

  filterSelectAll(valueList) {
    let newState = this.state.valueSelections;

    valueList.forEach((row) => {
      newState[row.value] = true;
    });

    this.setState({valueSelections: newState});
  }

  valueRowClick(value) {
    let newState = this.state.valueSelections;

    newState[value] = !newState[value];

    this.setState({valueSelections: newState});
  }

  applyDirective() {
    let condition;
    let column = this.props.column;
    let textValue = this.state.textFilter;

    switch (this.state.selectedCondition) {
      case 'EMPTY':
        condition = `filter-row-if-not-matched ${column} ^\s*$`;
        break;
      case 'NOTEMPTY':
        condition = `filter-row-if-matched ${column} ^\s*$`;
        break;
      case 'TEXTCONTAINS':
        condition = `filter-row-if-not-matched ${column} .*${textValue}.*`;
        break;
      case 'TEXTNOTCONTAIN':
        condition = `filter-row-if-matched ${column} .*${textValue}.*`;
        break;
      case 'TEXTSTARTSWITH':
        condition = `filter-row-if-false ${column} =^ "${textValue}"`;
        break;
      case 'TEXTENDSWITH':
        condition = `filter-row-if-false ${column} =$ "${textValue}"`;
        break;
      case 'TEXTEXACTLY':
        condition = `filter-row-if-not-matched ${column} ${textValue}`;
        break;
      case 'NONE':
        condition = this.createFilterValueCondition();
        break;
    }

    this.execute([condition]);
  }

  createFilterValueCondition() {
    let valueArray = Object.keys(this.state.valueSelections)
      .filter((row) => {
        return this.state.valueSelections[row];
      })
      .map((row) => {
        return `"${row}"`;
      })
      .join(', ');

    let column = this.props.column;

    let condition = `filter-row-if-false ${column} =~ [${valueArray}]`;

    return condition;
  }

  execute(addDirective) {
    execute(addDirective)
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
      }, (err) => {
        console.log('error', err);
        this.setState({
          error: err.message || err.response.message
        });
      });
  }

  renderTextFilter() {
    if (this.state.selectedCondition.substr(0, 4) !== 'TEXT') { return null; }

    return (
      <div>
        <br />
        <input
          type="text"
          className="form-control"
          value={this.state.textFilter}
          onChange={this.handleTextFilterChange}
          placeholder={T.translate(`${SUFFIX}.ByCondition.textPlaceholder`)}
        />
      </div>
    );
  }

  renderCondition() {
    let filterConditions = this.conditionsOptions.map((filter) => {
      return {
        filter: filter,
        displayText: T.translate(`${SUFFIX}.Conditions.${filter}`)
      };
    });

    return (
      <div>
        <h5>{T.translate(`${SUFFIX}.ByCondition.title`)}</h5>

        <div className="filter-condition">
          <div>
            <select
              className="form-control"
              value={this.state.selectedCondition}
              onChange={this.handleConditionSelect}
            >
              <option value="NONE">
                {T.translate(`${SUFFIX}.Conditions.NONE`)}
              </option>
              <option
                disabled="disabled"
                role="separator"
              >
                &#x2500;&#x2500;&#x2500;&#x2500;
              </option>
              {
                filterConditions.map((condition) => {
                  return (
                    <option
                      value={condition.filter}
                      key={condition.id}
                    >
                      {condition.displayText}
                    </option>
                  );
                })
              }
            </select>
          </div>

          {this.renderTextFilter()}

        </div>
      </div>
    );
  }

  renderValue() {
    let filteredValueList = this.valueList;

    if (this.state.filterValueSearch.length > 0) {
      filteredValueList = filteredValueList.filter((row) => {
        let value = row.value.toLowerCase();
        let search = this.state.filterValueSearch.toLowerCase();

        return value.indexOf(search) !== -1;
      });
    }

    return (
      <div>
        <h5>{T.translate(`${SUFFIX}.ByValue.title`)}</h5>

        <div className="filter-value">
          <div>
            <input
              type="text"
              className="form-control"
              value={this.state.filterValueSearch}
              onChange={this.handleFilterSearchChange}
              placeholder={T.translate(`${SUFFIX}.ByValue.searchPlaceholder`)}
            />
          </div>
          <div className="filter-selects">
            <span
              onClick={this.filterSelectAll.bind(this, filteredValueList)}
            >
              {T.translate(`${SUFFIX}.ByValue.selectAll`)}
            </span>
            <span> | </span>
            <span
              onClick={this.filterClearAll.bind(this, filteredValueList)}
            >
              {T.translate(`${SUFFIX}.ByValue.clearAll`)}
            </span>
          </div>

          <div className="value-options">
            {
              filteredValueList.map((row) => {
                return (
                  <div
                    key={row.id}
                    className="value-row"
                    onClick={this.valueRowClick.bind(this, row.value)}
                  >
                    <span
                      className={classnames('fa', {
                        'fa-check-square': this.state.valueSelections[row.value],
                        'fa-square': !this.state.valueSelections[row.value]
                      })}
                    />
                    <span title={row.value}>{row.value}</span>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="filterby-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        {this.renderCondition()}

        <hr />

        {this.renderValue()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
          >
            {T.translate('features.DataPrep.Directives.save')}
          </button>

          <button
            className="btn btn-link float-xs-right"
            onClick={this.props.close}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </div>

      </div>
    );

  }

  render() {
    return (
      <div
        className={classnames('filterby-column-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>{T.translate(`${SUFFIX}.title`)}</span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

FilterByDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
