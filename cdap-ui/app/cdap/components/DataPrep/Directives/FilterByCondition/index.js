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
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import T from 'i18n-react';

const SUFFIX = 'features.DataPrep.Directives.FilterByCondition';

export default class FilterByConditionDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedCondition: 'EMPTY',
      textFilter: ''
    };

    this.handleConditionSelect = this.handleConditionSelect.bind(this);
    this.handleTextFilterChange = this.handleTextFilterChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);

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

  componentDidUpdate() {
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0) {
      this.textFilterRef.focus();
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

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode !== 13 || this.state.textFilter.length === 0) { return; }

    this.applyDirective();
  }

  applyDirective() {
    let directive;
    let column = this.props.column;
    let textValue = this.state.textFilter;

    switch (this.state.selectedCondition) {
      case 'EMPTY':
        directive = `filter-row-if-not-matched ${column} ^\s*$`;
        break;
      case 'NOTEMPTY':
        directive = `filter-row-if-matched ${column} ^\s*$`;
        break;
      case 'TEXTCONTAINS':
        directive = `filter-row-if-not-matched ${column} .*${textValue}.*`;
        break;
      case 'TEXTNOTCONTAIN':
        directive = `filter-row-if-matched ${column} .*${textValue}.*`;
        break;
      case 'TEXTSTARTSWITH':
        directive = `filter-row-if-false ${column} =^ "${textValue}"`;
        break;
      case 'TEXTENDSWITH':
        directive = `filter-row-if-false ${column} =$ "${textValue}"`;
        break;
      case 'TEXTEXACTLY':
        directive = `filter-row-if-not-matched ${column} ${textValue}`;
        break;
    }

    this.execute([directive]);
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
          placeholder={T.translate(`${SUFFIX}.textPlaceholder`)}
          ref={ref => this.textFilterRef = ref}
          onKeyPress={this.handleKeyPress}
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
        <h5>{T.translate(`${SUFFIX}.title`)}</h5>

        <div className="filter-condition">
          <div>
            <select
              className="form-control"
              value={this.state.selectedCondition}
              onChange={this.handleConditionSelect}
            >
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

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    let disabled = this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0;

    return (
      <div
        className="filterby-condition-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        {this.renderCondition()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={disabled}
          >
            {T.translate('features.DataPrep.Directives.apply')}
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
        className={classnames('filterby-condition-directive clearfix action-item', {
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

FilterByConditionDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
