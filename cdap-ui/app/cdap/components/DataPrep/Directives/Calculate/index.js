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

import React, {Component, PropTypes} from 'react';
import classnames from 'classnames';
import T from 'i18n-react';
import {Input} from 'reactstrap';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import ScrollableList from 'components/ScrollableList';
import IconSVG from 'components/IconSVG';
import WarningContainer from 'components/WarningContainer';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {columnNameAlreadyExists} from 'components/DataPrep/helper';
import {preventPropagation} from 'services/helpers';
import capitalize from 'lodash/capitalize';
import Mousetrap from 'mousetrap';

require('./Calculate.scss');

const PREFIX = 'features.DataPrep.Directives.Calculate';
const COPY_NEW_COLUMN_PREFIX = 'features.DataPrep.DataPrepTable.copyToNewColumn';

export default class Calculate extends Component {
  constructor(props) {
    super(props);

    this.NUMBER_TYPES = ['integer', 'short', 'long', 'float', 'double'];
    this.VALID_TYPES = this.NUMBER_TYPES.concat(['string']);

    this.columnType = DataPrepStore.getState().dataprep.types[this.props.column];

    this.defaultState = {
      operationPopoverOpen: null,
      operationInput : 1,
      createNewColumn: false,
      newColumnInput: this.props.column + T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputSuffix`),
      isDisabled: this.VALID_TYPES.indexOf(this.columnType) === -1
    };

    this.state = Object.assign({}, this.defaultState);

    this.setOperationInput = this.setOperationInput.bind(this);
    this.setNewColumnInput = this.setNewColumnInput.bind(this);
    this.setDefaultState = this.setDefaultState.bind(this);
    this.toggleCreateNewColumn = this.toggleCreateNewColumn.bind(this);
    this.getExpressionAndApply = this.getExpressionAndApply.bind(this);

    this.CALCULATE_OPTIONS = [
      {
        name: 'label'
      },
      {
        name: 'ADD',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `${this.props.column} + ${this.state.operationInput}`
      },
      {
        name: 'SUBTRACT',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `${this.props.column} - ${this.state.operationInput}`
      },
      {
        name: 'MULTIPLY',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `${this.props.column} * ${this.state.operationInput}`
      },
      {
        name: 'DIVIDE',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `${this.props.column} / ${this.state.operationInput}`
      },
      {
        name: 'MODULO',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `${this.props.column} % ${this.state.operationInput}`
      },
      {
        name: 'divider',
        validColTypes: this.NUMBER_TYPES
      },
      {
        name: 'POWEROF',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, ${this.state.operationInput})`
      },
      {
        name: 'SQUARE',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, 2)`
      },
      {
        name: 'SQUAREROOT',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:sqrt(${this.props.column})`
      },
      {
        name: 'CUBE',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, 3)`
      },
      {
        name: 'CUBEROOT',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:cbrt(${this.props.column})`
      },
      {
        name: 'LOG',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:log10(${this.props.column})`
      },
      {
        name: 'NATURALLOG',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:log(${this.props.column})`
      },
      {
        name: 'divider',
        validColTypes: this.NUMBER_TYPES
      },
      {
        name: 'ABSVALUE',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:abs(${this.props.column})`
      },
      {
        name: 'CEIL',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:ceil(${this.props.column})`
      },
      {
        name: 'FLOOR',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:floor(${this.props.column})`
      },
      {
        name: 'divider',
        validColTypes: this.NUMBER_TYPES
      },
      {
        name: 'SIN',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:sin(${this.props.column})`
      },
      {
        name: 'COS',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:cos(${this.props.column})`
      },
      {
        name: 'TAN',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:tan(${this.props.column})`
      },
      {
        name: 'ARCCOS',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:acos(${this.props.column})`
      },
      {
        name: 'ARCSIN',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:asin(${this.props.column})`
      },
      {
        name: 'ARCTAN',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:atan(${this.props.column})`
      },
      {
        name: 'divider',
        validColTypes: this.NUMBER_TYPES
      },
      {
        name: 'ROUND',
        validColTypes: this.NUMBER_TYPES,
        expression: () => `math:round(${this.props.column})`
      },
      {
        name: 'RANDOM',
        validColTypes: this.NUMBER_TYPES,
        expression: () => 'math:random()'
      },
      {
        name: 'CHARCOUNT',
        validColTypes: ['string'],
        expression: () => `string:length(${this.props.column})`
      }
    ];

    this.SIMPLE_POPOVER_OPTIONS = ['SQUARE', 'SQUAREROOT', 'CUBE', 'CUBEROOT', 'LOG', 'NATURALLOG', 'ABSVALUE', 'CEIL', 'FLOOR', 'SIN', 'COS', 'TAN', 'ARCCOS', 'ARCSIN', 'ARCTAN', 'ROUND', 'RANDOM'];
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('calculate-directive'));
  }

  componentWillReceiveProps(nextProps) {
    if (!nextProps.isOpen) {
      this.setDefaultState();
    }
  }

  componentDidUpdate() {
    if (this.props.isOpen) {
      Mousetrap.bind('enter', this.getExpressionAndApply);
    }
    if (this.props.isOpen && !this.state.isDisabled && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  setDefaultState() {
    this.setState(this.defaultState);
  }

  setOperationInput(e) {
    let value = parseInt(e.target.value, 10);
    if (typeof value !== 'number' || isNaN(value)) {
      return;
    }

    this.setState({
      operationInput: value
    });
  }

  setNewColumnInput(e) {
    this.setState({
      newColumnInput: e.target.value
    });
  }

  toggleCreateNewColumn() {
    this.setState({
      createNewColumn: !this.state.createNewColumn
    });
  }

  popoverOptionClick(option) {
    let operationPopoverOpen = option === this.state.operationPopoverOpen ? null : option;

    let newColumnInput = this.defaultState.newColumnInput;

    if (option === 'CHARCOUNT') {
      newColumnInput = this.props.column + T.translate(`${PREFIX}.newColumnInputCountSuffix`);
    }

    let newState = Object.assign({}, this.defaultState, {
      operationPopoverOpen,
      newColumnInput
    });

    this.setState(newState);
  }

  getExpressionAndApply() {
    if (this.isApplyDisabled()) {
      return;
    }

    let expression;
    let operationObj = this.CALCULATE_OPTIONS.filter(option => this.state.operationPopoverOpen === option.name)[0];

    if (operationObj && typeof operationObj.expression === 'function') {
      expression = operationObj.expression();
    }

    this.applyDirective(expression);
  }

  isApplyDisabled() {
    return ((this.state.createNewColumn || this.state.operationPopoverOpen === 'CHARCOUNT') && this.state.newColumnInput.length === 0) ||
        (this.state.operationPopoverOpen === 'MODULO' && parseInt(this.state.operationInput , 10) < 1);
  }

  applyDirective(expression) {
    let destinationColumn = this.props.column;
    if ((this.state.createNewColumn || this.state.operationPopoverOpen === 'CHARCOUNT') && this.state.newColumnInput.length > 0) {
      destinationColumn = this.state.newColumnInput;
    }
    let directive = `set-column ${destinationColumn} ${expression}`;

    execute([directive])
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
      }, (err) => {
        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      }
    );
  }

  renderOptions() {
    return (
      this.CALCULATE_OPTIONS
        .filter(option => option.name === 'label' || option.validColTypes.indexOf(this.columnType) !== -1)
        .map((option) => {
          if (option.name === 'label') {
            return (
              <div className="column-type-label">
                <span>
                  {
                    this.NUMBER_TYPES.indexOf(this.columnType) !== -1 ?
                      T.translate(`${PREFIX}.columnTypeLabel.numeric`)
                    :
                      capitalize(this.columnType)
                  }
                </span>
              </div>
            );
          }
          if (option.name === 'divider') {
            return (
              <div className="column-action-divider calculate-options-divider">
                <hr />
              </div>
            );
          }
          return (
            <div
              key={option.name}
              className={classnames('option', {
                'active': this.state.operationPopoverOpen === option.name
              })}
              onClick={this.popoverOptionClick.bind(this, option.name)}
            >
              <span>
                {T.translate(`${PREFIX}.OptionsLabels.${option.name}`)}
              </span>
              <span className="float-xs-right">
                <span className="fa fa-caret-right" />
              </span>
              {
                this.state.operationPopoverOpen === option.name ?
                  this.renderOperationPopover()
                : null
              }
            </div>
          );
        })
    );
  }

  renderOptionsWithScrollableList() {
    return (
      <ScrollableList>
        {this.renderOptions()}
      </ScrollableList>
    );
  }

  renderDetail() {
    if (!this.props.isOpen || this.state.isDisabled) { return null; }

    return (
      <div
        className="second-level-popover"
        onClick={preventPropagation}
      >
        <div className="calculate-options">
          {
            /* Right now don't need to use ScrollableList for string options since there's only one of them.
            When we have more string options in the future, we can just do renderOptions() */

            this.NUMBER_TYPES.indexOf(this.columnType) !== -1 ?
              this.renderOptionsWithScrollableList()
            :
              this.renderOptions()
          }
        </div>
      </div>
    );
  }

  renderNewColumnNameInputWithCheckbox() {
    return (
      <div>
        <div
          className="create-new-column-line"
          onClick={this.toggleCreateNewColumn}
        >
          <span className="fa fa-fw">
            <IconSVG
              name={this.state.createNewColumn ? 'icon-check-square' : 'icon-square-o'}
            />
          </span>

          <span>
            {T.translate(`${COPY_NEW_COLUMN_PREFIX}.label`)}
          </span>
        </div>
        {this.renderNewColumnNameInput()}
      </div>
    );
  }

  renderNewColumnNameInput() {
    if (!this.state.createNewColumn && this.state.operationPopoverOpen !== 'CHARCOUNT') { return null; }

    let inputLabel = T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputLabel`);
    let inputPlaceholder = T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputPlaceholder`);

    if (this.state.operationPopoverOpen === 'CHARCOUNT') {
      inputLabel = T.translate(`${PREFIX}.destinationColumnInputLabel`);
    }

    return (
      <div>
        <div className={classnames({"bigger-new-column-label": this.state.operationPopoverOpen === 'CHARCOUNT'})}>
          {inputLabel}
        </div>
        <Input
          type="text"
          className="form-control mousetrap"
          value={this.state.newColumnInput}
          onChange={this.setNewColumnInput}
          placeholder={inputPlaceholder}
          autoFocus
        />
        {
          columnNameAlreadyExists(this.state.newColumnInput) ? (
            <WarningContainer
              message={T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputDuplicate`)}
            />
          ) : null
        }
      </div>
    );
  }

  renderActionButtons() {
    return (
      <div className="action-buttons">
        <button
          className="btn btn-primary float-xs-left"
          disabled={this.isApplyDisabled()}
          onClick={this.getExpressionAndApply}
        >
          {T.translate('features.DataPrep.Directives.apply')}
        </button>

        <button
          className="btn btn-link float-xs-right"
          onClick={this.setDefaultState}
        >
          {T.translate('features.DataPrep.Directives.cancel')}
        </button>
      </div>
    );
  }

  renderOperationPopover() {
    if (!this.state.operationPopoverOpen) { return null; }

    if (this.state.operationPopoverOpen === 'CHARCOUNT') {
      return (
        <div
          className="third-level-popover"
          onClick={preventPropagation}
        >
          {this.renderNewColumnNameInput()}
          <hr />
          {this.renderActionButtons()}
        </div>
      );
    }

    if (this.SIMPLE_POPOVER_OPTIONS.indexOf(this.state.operationPopoverOpen) !== -1) {
      return (
        <div
          className="third-level-popover"
          onClick={preventPropagation}
        >
          {this.renderNewColumnNameInputWithCheckbox()}
          <hr />
          {this.renderActionButtons()}
        </div>
      );
    }

    let operationSign;
    let inputMin;

    if (this.state.operationPopoverOpen === 'MODULO') {
      inputMin = 1;
    }

    switch (this.state.operationPopoverOpen) {
      case 'ADD':
        operationSign = '+';
        break;
      case 'SUBTRACT':
        operationSign = '-';
        break;
      case 'MULTIPLY':
        operationSign = 'x';
        break;
      case 'DIVIDE':
        operationSign = '/';
        break;
      case 'MODULO':
        operationSign = '%';
        break;
    }

    return (
      <div
        className="third-level-popover"
        onClick={preventPropagation}
      >
        {
          this.state.operationPopoverOpen === 'POWEROF' ?
            (
              <div className="operation-description">
                {T.translate(`${PREFIX}.Options.POWEROF.description`)}
              </div>
            )
          : null
        }
        <div className="operation-sign-input">
          <span className="operation-sign">
            {operationSign}
          </span>
          <Input
            className="operation-input mousetrap"
            value={this.state.operationInput}
            onChange={this.setOperationInput}
            type="number"
            min={inputMin}
            autoFocus
          />
        </div>
        {this.renderNewColumnNameInputWithCheckbox()}

        <hr />

        {this.renderActionButtons()}
      </div>
    );
  }

  render() {
    let id = 'calculate-directive';

    return (
      <div>
        <div
          id={id}
          className={classnames('clearfix action-item', {
            'active': this.props.isOpen && !this.state.isDisabled,
            'disabled': this.state.isDisabled
          })}
        >
          <span>
            {T.translate(`${PREFIX}.title`)}
          </span>

          <span className="float-xs-right">
            <span className="fa fa-caret-right" />
          </span>

          {this.renderDetail()}
        </div>
        {
          this.state.isDisabled ? (
            <UncontrolledTooltip
              target={id}
              delay={{show: 250, hide: 0}}
            >
              {T.translate(`${PREFIX}.disabledTooltip`)}
            </UncontrolledTooltip>
          ) : null
        }
      </div>
    );
  }
}
Calculate.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.string,
  close: PropTypes.func,
  onComplete: PropTypes.func
};
