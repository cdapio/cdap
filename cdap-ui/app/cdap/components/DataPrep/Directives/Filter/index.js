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
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

require('./FilterDirective.scss');

const SUFFIX = 'features.DataPrep.Directives.Filter';

const DIRECTIVES_MAP = {
  'KEEP': {
    'EMPTY': 'filter-row-if-not-matched',
    'TEXTEXACTLY': 'filter-rows-on regex-not-match',
    'TEXTCONTAINS': 'filter-rows-on regex-not-match',
    'TEXTSTARTSWITH': 'filter-rows-on condition-false',
    'TEXTENDSWITH': 'filter-rows-on condition-false',
    'TEXTREGEX': 'filter-rows-on regex-not-match',
    'CUSTOMCONDITION': 'filter-rows-on condition-false'
  },
  'REMOVE': {
    'EMPTY': 'filter-row-if-matched',
    'TEXTEXACTLY': 'filter-rows-on regex-match',
    'TEXTCONTAINS': 'filter-rows-on regex-match',
    'TEXTSTARTSWITH': 'filter-rows-on condition-true',
    'TEXTENDSWITH': 'filter-rows-on condition-true',
    'TEXTREGEX': 'filter-rows-on regex-match',
    'CUSTOMCONDITION': 'filter-rows-on condition-true'
  }
};

export default class FilterDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedCondition: 'EMPTY',
      textFilter: '',
      rowFilter: 'KEEP',
      customFilter: ''
    };

    this.handleConditionSelect = this.handleConditionSelect.bind(this);
    this.handleTextFilterChange = this.handleTextFilterChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
    this.handleCustomFilterChange = this.handleCustomFilterChange.bind(this);

    this.conditionsOptions = [
      'EMPTY',
      'TEXTEXACTLY',
      'TEXTCONTAINS',
      'TEXTSTARTSWITH',
      'TEXTENDSWITH',
      'TEXTREGEX'
    ];
  }

  componentDidUpdate() {
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0 && this.textFilterRef) {
      this.textFilterRef.focus();
    } else if (this.state.selectedCondition.substr(0, 6) === 'CUSTOM' && this.state.customFilter.length === 0) {
      this.customFilterRef.focus();
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

  handleCustomFilterChange(e) {
    this.setState({customFilter: e.target.value});
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode !== 13 || this.state.textFilter.length === 0) { return; }

    this.applyDirective();
  }

  handleRowFilter(type) {
    this.setState({rowFilter: type});
  }

  applyDirective() {
    let directive;
    let column = this.props.column;
    let textValue = this.state.textFilter;

    let condition = DIRECTIVES_MAP[this.state.rowFilter][this.state.selectedCondition];

    switch (this.state.selectedCondition) {
      case 'EMPTY':
        directive = `${condition} ${column} ^\\s*$`;
        break;
      case 'TEXTCONTAINS':
        directive = `${condition} ${column} .*${textValue}.*`;
        break;
      case 'TEXTSTARTSWITH':
        directive = `${condition} ${column} =^ "${textValue}"`;
        break;
      case 'TEXTENDSWITH':
        directive = `${condition} ${column} =$ "${textValue}"`;
        break;
      case 'TEXTEXACTLY':
        directive = `${condition} ${column} ${textValue}`;
        break;
      case 'TEXTREGEX':
        directive = `${condition} ${column} ${textValue}`;
        break;
      case 'CUSTOMCONDITION':
        directive = `${condition} ${this.state.customFilter}`;
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

        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      });
  }

  renderCustomFilter() {
    if (this.state.selectedCondition.substr(0, 6) !== 'CUSTOM') { return null; }

    return (
      <div>
        <br />
        <textarea
          className="form-control"
          value={this.state.customFilter}
          onChange={this.handleCustomFilterChange}
          ref={ref => this.customFilterRef = ref}
          placeholder={T.translate(`${SUFFIX}.Placeholders.CUSTOMCONDITION`, {columnName: this.props.column})}
        />
      </div>
    );
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
          placeholder={T.translate(`${SUFFIX}.Placeholders.${this.state.selectedCondition}`)}
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
        <div className="row-filter-container">
          <span
            className={classnames('cursor-pointer row-filter', {
              'active': this.state.rowFilter === 'KEEP'
            })}
            onClick={this.handleRowFilter.bind(this, 'KEEP')}
          >
            {T.translate(`${SUFFIX}.KEEP`)}
          </span>
          <span> | </span>
          <span
            className={classnames('cursor-pointer row-filter', {
              'active': this.state.rowFilter === 'REMOVE'
            })}
            onClick={this.handleRowFilter.bind(this, 'REMOVE')}
          >
            {T.translate(`${SUFFIX}.REMOVE`)}
          </span>
        </div>

        <div className="filter-condition">
          <div className="condition-select">
            <span>{T.translate(`${SUFFIX}.if`)}</span>
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
                        key={condition.filter}
                      >
                        {condition.displayText}
                      </option>
                    );
                  })
                }
                <option
                  disabled="disabled"
                  role="separator"
                >
                  &#x2500;&#x2500;&#x2500;&#x2500;
                </option>
                <option value="CUSTOMCONDITION">
                  {T.translate(`${SUFFIX}.Conditions.CUSTOMCONDITION`)}
                </option>
              </select>
            </div>
          </div>

          {this.renderTextFilter()}
          {this.renderCustomFilter()}

        </div>
      </div>
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    let disabled = this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0;

    return (
      <div
        className="filter-detail second-level-popover"
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
        className={classnames('filter-directive clearfix action-item', {
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

FilterDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
