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

import React, {PropTypes, Component} from 'react';
import classnames from 'classnames';
import T from 'i18n-react';
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';
import {preventPropagation} from 'services/helpers';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import IconSVG from 'components/IconSVG';
import MouseTrap from 'mousetrap';

require('./MarkAsError.scss');

const PREFIX = 'features.DataPrep.Directives.MarkAsError';
const conditionsOptions = [
  'EMPTY',
  'TEXTEXACTLY',
  'TEXTCONTAINS',
  'TEXTSTARTSWITH',
  'TEXTENDSWITH',
  'TEXTREGEX',
  'divider',
  'ISNUMBER',
  'ISNOTNUMBER',
  'ISDOUBLE',
  'ISNOTDOUBLE',
  'ISINTEGER',
  'ISNOTINTEGER',
  'ISBOOLEAN',
  'ISNOTBOOLEAN',
  'divider',
  'ISDATE',
  'ISNOTDATE',
  'ISTIME',
  'ISNOTTIME',
  'divider',
  'CUSTOMCONDITION'
];
export default class MarkAsError extends Component {
  state = {
    selectedCondition: conditionsOptions[0],
    conditionValue: '',
    customCondition: `${this.props.column} == 0`,
    ignoreCase: false
  };

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('mark-as-error-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
      MouseTrap.bind('enter', this.applyDirective);
    }
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.conditionValue.length === 0 && this.conditionValueRef) {
      this.conditionValueRef.focus();
    } else if (this.state.selectedCondition.substr(0, 6) === 'CUSTOM' && this.state.customCondition.length === 0 && this.customConditionRef) {
      this.customConditionRef.focus();
    }
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
    MouseTrap.unbind('enter');
  }

  applyDirective = () => {
    if (this.isApplyDisabled()) {
      return;
    }
    MouseTrap.unbind('enter');
    execute([this.getDirectiveExpression()])
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
  };

  handleConditionValueChange = (e) => {
    this.setState({conditionValue: e.target.value});
  };

  handleCustomFilterChange = (e) => {
    this.setState({customCondition: e.target.value});
  };

  toggleIgnoreCase = () => {
    this.setState({
      ignoreCase: !this.state.ignoreCase
    });
  };

  getDQFunction(condition) {
    let conditionToFnMap = {
      'ISNUMBER' : 'isNumber',
      'ISINTEGER' : 'isInteger',
      'ISDOUBLE' : 'isDouble',
      'ISBOOLEAN' : 'isBoolean',
      'ISDATE' : 'isDate',
      'ISTIME' : 'isTime'
    };
    let c = condition.replace('NOT', '');
    return conditionToFnMap[c];
  }

  getDirectiveExpression = () => {
    let directive = 'send-to-error';
    let condition;
    let column = this.props.column;
    let textValue = this.state.conditionValue;
    let equalityOperator = '==';
    let finalExpression;

    switch (this.state.selectedCondition) {
      case 'EMPTY':
        finalExpression = `${directive} empty(${column})`;
        break;
      case 'TEXTCONTAINS':
        if (this.state.ignoreCase) {
          textValue = `(?i).*${textValue}`;
        } else {
          textValue = `.*${textValue}`;
        }
        finalExpression = `${directive} ${column} =~ "${textValue}.*"`;
        break;
      case 'TEXTSTARTSWITH':
        equalityOperator = '=^';
        if (this.state.ignoreCase) {
          textValue = `(?i)^${textValue}.*`;
          equalityOperator = '=~';
        }
        finalExpression = `${directive} ${column} ${equalityOperator} "${textValue}"`;
        break;
      case 'TEXTENDSWITH':
        equalityOperator = '=$';
        if (this.state.ignoreCase) {
          textValue = `(?i).*${textValue}$`;
          equalityOperator = '=~';
        }
        finalExpression = `${directive} ${column} ${equalityOperator} "${textValue}"`;
        break;
      case 'TEXTEXACTLY':
        if (this.state.ignoreCase) {
          textValue = `(?i)${textValue}`;
          equalityOperator = `=~`;
        }
        finalExpression = `${directive} ${column} ${equalityOperator} "${textValue}"`;
        break;
      case 'TEXTREGEX':
        finalExpression = `${directive} ${column} =~ "${textValue}"`;
        break;
      case 'CUSTOMCONDITION':
        finalExpression = `${directive} ${this.state.customCondition}`;
        break;
      case 'ISNUMBER':
      case 'ISNOTNUMBER':
      case 'ISINTEGER':
      case 'ISNOTINTEGER':
      case 'ISDOUBLE':
      case 'ISNOTDOUBLE':
      case 'ISBOOLEAN':
      case 'ISNOTBOOLEAN':
      case 'ISDATE':
      case 'ISNOTDATE':
      case 'ISTIME':
      case 'ISNOTTIME':
        condition = `dq:${this.getDQFunction(this.state.selectedCondition)}(${column})`;
        if (this.state.selectedCondition.indexOf('NOT') !== -1) {
          condition = `!${condition}`;
        }
        finalExpression = `${directive} ${condition}`;
        break;
    }
    return finalExpression;
  }

  renderTextCondition = () => {
    if (this.state.selectedCondition.substr(0, 4) !== 'TEXT') { return null; }

    let ignoreCase;
    if (this.state.selectedCondition !== 'TEXTREGEX') {
      ignoreCase = (
        <div>
          <span
            className="cursor-pointer"
            onClick={this.toggleIgnoreCase}
          >
            <IconSVG
              className="fa"
              name={this.state.ignoreCase ? "icon-check-square-o" : "icon-square-o"}
            />
            <span>
              {T.translate(`${PREFIX}.ignoreCase`)}
            </span>
          </span>
        </div>
      );
    }

    return (
      <div>
        <br />
        <div>
          <input
            type="text"
            className="form-control mousetrap"
            value={this.state.conditionValue}
            onChange={this.handleConditionValueChange}
            placeholder={T.translate(`${PREFIX}.Placeholders.${this.state.selectedCondition}`)}
            ref={ref => this.conditionValueRef = ref}
          />
        </div>
        {ignoreCase}
      </div>
    );
  };

  renderCustomFilter = () => {
    if (this.state.selectedCondition.substr(0, 6) !== 'CUSTOM') { return null; }

    return (
      <div>
        <br />
        <textarea
          className="form-control custom-condition-input"
          value={this.state.customCondition}
          onChange={this.handleCustomFilterChange}
          ref={ref => this.customConditionRef = ref}
          placeholder={T.translate(`${PREFIX}.Placeholders.CUSTOMCONDITION`, {column: this.props.column})}
        />
      </div>
    );
  };

  setCondition = (e) => {
    this.setState({
      selectedCondition: e.target.value
    });
  };

  renderCondition = () => {
    let markAsConditions = conditionsOptions.map((id) => {
      return {
        id,
        displayText: T.translate(`${PREFIX}.Conditions.${id}`)
      };
    });
    return (
      <div>
        <div className="mark-as-error-condition">
          <div className="condition-select">
            <strong>{T.translate(`${PREFIX}.if`)}</strong>
            <div>
              <select
                className="form-control mousetrap"
                value={this.state.selectedCondition}
                onChange={this.setCondition}
              >
                {
                  markAsConditions.map(condition => {
                    if (condition.id === 'divider') {
                      return (
                        <option
                          disabled="disabled"
                          role="separator"
                        >
                          &#x2500;&#x2500;&#x2500;&#x2500;
                        </option>
                      );
                    }
                    return (
                      <option
                        value={condition.id}
                        key={condition.id}
                      >
                        {condition.displayText}
                      </option>
                    );
                  })
                }
              </select>
            </div>
          </div>
        </div>
        {this.renderTextCondition()}
        {this.renderCustomFilter()}
      </div>
    );
  };

  isApplyDisabled = () => {
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT') {
      return this.state.conditionValue.length === 0;
    }

    if (this.state.selectedCondition.substr(0, 6) === 'CUSTOM') {
      return this.state.customCondition.length === 0;
    }
  };

  renderDetail = () => {
    if (!this.props.isOpen) {
      MouseTrap.unbind('enter');
      return null;
    }

    return (
      <div
        className="filter-detail second-level-popover"
        onClick={preventPropagation}
      >
        {this.renderCondition()}

        <div className="mark-as-error-tooltip">
          <span>{T.translate(`${PREFIX}.tooltip`)}</span>
        </div>
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
  };

  render() {
    return (
      <div
        id="mark-as-error-directive"
        className={classnames('clearfix action-item', {
          'active': this.state.isOpen
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className="float-xs-right">
          <IconSVG
            name="icon-caret-right"
            className="fa"
          />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

MarkAsError.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
