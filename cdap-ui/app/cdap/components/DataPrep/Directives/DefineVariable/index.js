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
import T from 'i18n-react';
import classnames from 'classnames';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import Mousetrap from 'mousetrap';
import {preventPropagation} from 'services/helpers';

require('./DefineVariable.scss');

const PREFIX = `features.DataPrep.Directives.DefineVariable`;

export default class DefineVariableDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedCondition: 'TEXTEXACTLY',
      textFilter: '',
      customFilter: '',
      variableName: '',
      selectedColumn: this.props.column
    };

    this.applyDirective = this.applyDirective.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
    this.isApplyDisabled = this.isApplyDisabled.bind(this);

    this.conditionsOptions = [
      'TEXTEXACTLY',
      'TEXTSTARTSWITH',
      'TEXTENDSWITH',
      'TEXTREGEX'
    ];
  }

  componentWillMount() {
    this.columnsList = DataPrepStore.getState().dataprep.headers;
  }

  componentDidMount() {
    let directiveElem = document.getElementById('set-variable-directive');
    this.calculateOffset = setPopoverOffset.bind(this, directiveElem);

    this.mousetrap = new Mousetrap(directiveElem);
    this.mousetrap.bind('enter', this.applyDirective);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentWillUnmount() {
    this.mousetrap.reset();
    delete this.mousetrap;
  }

  handleStateValueChange(key, e) {
    this.setState({
      [key]: e.target.value
    });
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode !== 13 || this.isApplyDisabled()) { return; }

    this.applyDirective();
  }

  applyDirective() {
    let condition = 'set-variable';
    let column = this.props.column;
    let textValue = this.state.textFilter;
    let variableName = this.state.variableName;
    let selectedColumn = this.state.selectedColumn;
    let directive;

    if (!textValue || !variableName) { return; }

    switch (this.state.selectedCondition) {
      case 'TEXTCONTAINS':
        directive = `${condition} ${variableName} ${column} =~ .*${textValue}.* ? ${selectedColumn} : ${variableName}`;
        break;
      case 'TEXTSTARTSWITH':
        directive = `${condition} ${variableName} ${column} =^ "${textValue}" ? ${selectedColumn} : ${variableName}`;
        break;
      case 'TEXTENDSWITH':
        directive = `${condition} ${variableName} ${column} =$ "${textValue}" ? ${selectedColumn} : ${variableName}`;
        break;
      case 'TEXTEXACTLY':
        directive = `${condition} ${variableName} ${column} == "${textValue}" ? ${selectedColumn} : ${variableName}`;
        break;
      case 'TEXTREGEX':
        directive = `${condition} ${variableName} ${column} =~ ${textValue} ? ${selectedColumn} : ${variableName}`;
        break;
      case 'CUSTOMCONDITION':
        directive = `${condition} ${variableName} ${this.state.customFilter} ? ${selectedColumn} : ${variableName}`;
        break;
    }

    this.execute([directive]);
  }

  execute(addDirective) {
    Mousetrap.unbind('enter', this.applyDirective);
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

  isApplyDisabled() {
    let disabled = this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0;
    disabled = disabled || this.state.variableName.length === 0;
    return disabled;
  }

  renderCustomFilter() {
    if (this.state.selectedCondition.substr(0, 6) !== 'CUSTOM') { return null; }

    return (
      <div className="condition-text-input">
        <textarea
          className="form-control"
          value={this.state.customFilter}
          onChange={this.handleStateValueChange.bind(this, 'customFilter')}
          ref={ref => this.customFilterRef = ref}
          placeholder={T.translate(`${PREFIX}.Placeholders.CUSTOMCONDITION`)}
        />
      </div>
    );
  }

  renderTextFilter() {
    if (this.state.selectedCondition.substr(0, 4) !== 'TEXT') { return null; }

    return (
      <div className="condition-text-input">
        <input
          type="text"
          className="form-control mousetrap"
          value={this.state.textFilter}
          onChange={this.handleStateValueChange.bind(this, 'textFilter')}
          placeholder={T.translate(`${PREFIX}.Placeholders.${this.state.selectedCondition}`)}
          ref={ref => this.textFilterRef = ref}
          onKeyPress={this.handleKeyPress}
        />
      </div>
    );
  }

  renderCondition() {
    let selectConditions = this.conditionsOptions.map((filter) => {
      return {
        filter: filter,
        displayText: T.translate(`${PREFIX}.Conditions.${filter}`)
      };
    });

    return (
      <div>
        <div className="select-condition">
          <div className="condition-select">
            <h5>
              {T.translate(`${PREFIX}.step2`)}
            </h5>

            <label className="control-label">
              {T.translate(`${PREFIX}.if`)}
            </label>
            <div>
              <select
                className="form-control mousetrap"
                value={this.state.selectedCondition}
                onChange={this.handleStateValueChange.bind(this, 'selectedCondition')}
              >
                {
                  selectConditions.map((condition) => {
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
                  {T.translate(`${PREFIX}.Conditions.CUSTOMCONDITION`)}
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

  renderVariableName() {
    return (
      <div>
        <h5>
          {T.translate(`${PREFIX}.step1`)}
        </h5>

        <div>
          <input
            type="text"
            className="form-control"
            value={this.state.variableName}
            onChange={this.handleStateValueChange.bind(this, 'variableName')}
            placeholder={T.translate(`${PREFIX}.variableNamePlaceholder`)}
            onKeyPress={this.handleKeyPress}
          />
        </div>
      </div>
    );
  }

  renderVariableDetail() {
    return (
      <div className="variable-detail">
        <div>
          <label className="control-label select-column-label">
            {T.translate(`${PREFIX}.selectColumnLabel`)}
          </label>

          <div>
            <select
              className="form-control"
              value={this.state.selectedColumn}
              onChange={this.handleStateValueChange.bind(this, 'selectedColumn')}
            >
              {
                this.columnsList.map((column) => {
                  return (
                    <option
                      value={column}
                      key={column}
                    >
                      {column}
                    </option>
                  );
                })
              }
            </select>
          </div>
        </div>
      </div>
    );
  }

  renderSummary() {
    if (!this.state.textFilter || !this.state.variableName) { return null; }

    return (
      <div className="summary">
        <strong className="summary-label">
          {T.translate(`${PREFIX}.summaryLabel`)}
        </strong>
        <span>
          {T.translate(`${PREFIX}.summaryText`, {
            variableName: this.state.variableName,
            condition: T.translate(`${PREFIX}.Conditions.${this.state.selectedCondition}`),
            value: this.state.textFilter,
            columnName: this.props.column,
            selectedColumn: this.state.selectedColumn
          })}
        </span>
      </div>
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="set-variable-detail second-level-popover"
        onClick={preventPropagation}
      >
        {this.renderVariableName()}

        <hr />

        {this.renderCondition()}

        <br />

        {this.renderVariableDetail()}

        {this.renderSummary()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={this.isApplyDisabled()}
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
        id="set-variable-directive"
        className={classnames('set-variable-directive clearfix action-item', {
          'active': this.state.isOpen
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

DefineVariableDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
