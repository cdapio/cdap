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
import T from 'i18n-react';
import classnames from 'classnames';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';

require('./SetCounter.scss');

const PREFIX = `features.DataPrep.Directives.SetCounter`;

export default class SetCounterDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedCondition: 'ALWAYS',
      ifCondition: '',
      variableName: '',
      incrementBy: 1
    };

    this.applyDirective = this.applyDirective.bind(this);

    this.conditions = [
      'ALWAYS',
      'IFCONDITION'
    ];

  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('set-counter-directive'));
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  handleStateValueChange(key, e) {
    this.setState({
      [key]: e.target.value
    });
  }

  applyDirective() {
    let variableName = this.state.variableName;
    let incrementBy = this.state.incrementBy;

    let expression = this.state.selectedCondition === 'ALWAYS' ? 'true' : this.state.ifCondition;

    let directive = `increment-variable ${variableName} ${incrementBy} ${expression}`;

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

  renderCustomCondition() {
    if (this.state.selectedCondition !== 'IFCONDITION') { return null; }

    return (
      <textarea
        className="form-control"
        value={this.state.ifCondition}
        onChange={this.handleStateValueChange.bind(this, 'ifCondition')}
        placeholder={T.translate(`${PREFIX}.ifConditionPlaceholder`)}
      />
    );
  }

  renderCondition() {
    return (
      <div className="condition-container">
        <select
          className="form-control"
          value={this.state.selectedCondition}
          onChange={this.handleStateValueChange.bind(this, 'selectedCondition')}
        >
          {
            this.conditions.map((condition) => {
              return (
                <option value={condition}>
                  {T.translate(`${PREFIX}.Conditions.${condition}`)}
                </option>
              );
            })
          }
        </select>

        {this.renderCustomCondition()}
      </div>
    );
  }

  renderIncrementCounter() {
    return (
      <div>
        <label className="control-label">
          {T.translate(`${PREFIX}.incrementCounterLabel`)}
        </label>

        <input
          type="number"
          className="form-control"
          value={this.state.incrementBy}
          onChange={this.handleStateValueChange.bind(this, 'incrementBy')}
        />
      </div>
    );
  }

  renderVariableName() {
    return (
      <div>
        <label className="control-label">
          {T.translate(`${PREFIX}.variableNameLabel`)}
        </label>

        <input
          type="text"
          className="form-control"
          value={this.state.variableName}
          onChange={this.handleStateValueChange.bind(this, 'variableName')}
          placeholder={T.translate(`${PREFIX}.variableNamePlaceholder`)}
        />
      </div>
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    let disabled = this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.textFilter.length === 0;

    return (
      <div
        className="set-variable-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        {this.renderCondition()}

        {this.renderIncrementCounter()}

        <hr />

        {this.renderVariableName()}

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
        id="set-counter-directive"
        className={classnames('set-counter-directive clearfix action-item', {
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

SetCounterDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
