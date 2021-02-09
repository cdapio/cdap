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
import classnames from 'classnames';
import T from 'i18n-react';
import { Input } from 'reactstrap';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import { setPopoverOffset } from 'components/DataPrep/helper';
import ScrollableList from 'components/ScrollableList';
import IconSVG from 'components/IconSVG';
import WarningContainer from 'components/WarningContainer';
import { UncontrolledTooltip } from 'components/UncontrolledComponents';
import { columnNameAlreadyExists } from 'components/DataPrep/helper';
import { preventPropagation } from 'services/helpers';
import { NUMBER_TYPES, NATIVE_NUMBER_TYPES } from 'services/global-constants';
import capitalize from 'lodash/capitalize';
import Mousetrap from 'mousetrap';

require('./Calculate.scss');

const PREFIX = 'features.DataPrep.Directives.Calculate';
const COPY_NEW_COLUMN_PREFIX = 'features.DataPrep.DataPrepTable.copyToNewColumn';

export default class Calculate extends Component {
  constructor(props) {
    super(props);
    this.VALID_TYPES = NUMBER_TYPES.concat(['string']);

    const {isDisabled, crossColumn, columns, columnType} = this.parseColumns();

    this.isDisabled = isDisabled;
    this.crossColumn = crossColumn;
    this.columns = columns;
    this.columnType = columnType;

    this.defaultState = {
      operationPopoverOpen: null,
      operationInput: 1,
      createNewColumn: false,
      newColumnInput: this.props.column + T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputSuffix`),
      isDisabled: this.VALID_TYPES.indexOf(this.columnType) === -1 || this.isDisabled,
    };

    this.state = Object.assign({}, this.defaultState);

    this.setOperationInput = this.setOperationInput.bind(this);
    this.setNewColumnInput = this.setNewColumnInput.bind(this);
    this.setDefaultState = this.setDefaultState.bind(this);
    this.toggleCreateNewColumn = this.toggleCreateNewColumn.bind(this);
    this.getExpressionAndApply = this.getExpressionAndApply.bind(this);

    this.CALCULATE_OPTIONS = [
      {
        name: 'label',
      },
      {
        name: 'ADD',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `${this.props.column} + ${this.state.operationInput}`,
      },
      {
        name: 'SUBTRACT',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `${this.props.column} - ${this.state.operationInput}`,
      },
      {
        name: 'MULTIPLY',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `${this.props.column} * ${this.state.operationInput}`,
      },
      {
        name: 'DIVIDE',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `${this.props.column} / ${this.state.operationInput}`,
      },
      {
        name: 'MODULO',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `${this.props.column} % ${this.state.operationInput}`,
      },
      {
        name: 'DECIMALADD',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:add(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALSUBTRACT',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:subtract(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALMULTIPLY',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:multiply(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALDIVIDEQ',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:divideq(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALDIVIDER',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:divider(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'divider',
        validColTypes: NUMBER_TYPES,
      },
      {
        name: 'POWEROF',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'SQUARE',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, 2)`,
      },
      {
        name: 'SQUAREROOT',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:sqrt(${this.props.column})`,
      },
      {
        name: 'CUBE',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:pow(${this.props.column}, 3)`,
      },
      {
        name: 'CUBEROOT',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:cbrt(${this.props.column})`,
      },
      {
        name: 'LOG',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:log10(${this.props.column})`,
      },
      {
        name: 'NATURALLOG',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:log(${this.props.column})`,
      },
      {
        name: 'divider',
        validColTypes: NATIVE_NUMBER_TYPES,
      },
      {
        name: 'ABSVALUE',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:abs(${this.props.column})`,
      },
      {
        name: 'CEIL',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:ceil(${this.props.column})`,
      },
      {
        name: 'FLOOR',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:floor(${this.props.column})`,
      },
      {
        name: 'divider',
        validColTypes: NATIVE_NUMBER_TYPES,
      },
      {
        name: 'SIN',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:sin(${this.props.column})`,
      },
      {
        name: 'COS',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:cos(${this.props.column})`,
      },
      {
        name: 'TAN',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:tan(${this.props.column})`,
      },
      {
        name: 'ARCCOS',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:acos(${this.props.column})`,
      },
      {
        name: 'ARCSIN',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:asin(${this.props.column})`,
      },
      {
        name: 'ARCTAN',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:atan(${this.props.column})`,
      },
      {
        name: 'divider',
        validColTypes: NATIVE_NUMBER_TYPES,
      },
      {
        name: 'ROUND',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => `math:round(${this.props.column})`,
      },
      {
        name: 'RANDOM',
        validColTypes: NATIVE_NUMBER_TYPES,
        expression: () => 'math:random()',
      },
      {
        name: 'CHARCOUNT',
        validColTypes: ['string'],
        expression: () => `string:length(${this.props.column})`,
      },
      {
        name: 'PRECISION',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:precision(${this.props.column})`,
      },
      {
        name: 'SCALE',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:scale(${this.props.column})`,
      },
      {
        name: 'UNSCALED',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:unscaled(${this.props.column})`,
      },
      {
        name: 'DECIMALLEFT',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:decimal_left(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALRIGHT',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:decimal_right(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALABSVALUE',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:abs(${this.props.column})`,
      },
      {
        name: 'DECIMALPOWEROF',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:pow(${this.props.column}, ${this.state.operationInput})`,
      },
      {
        name: 'DECIMALSQUARE',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:pow(${this.props.column}, 2)`,
      },
      {
        name: 'DECIMALCUBE',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:pow(${this.props.column}, 3)`,
      },
      {
        name: 'NEGATE',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:negate(${this.props.column})`,
      },
      {
        name: 'STRIPZERO',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:strip_zero(${this.props.column})`,
      },
      {
        name: 'SIGN',
        validColTypes: ['bigdecimal'],
        expression: () => `decimal:sign(${this.props.column})`,
      },
    ];

    this.CROSS_COLUMN_CALCULATE_OPTIONS = [
      {
        name: 'CROSSADD',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:add(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSSUBTRACT',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:minus(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSMULTIPLY',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:multiply(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSDIVIDEQ',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:divideq(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSDIVIDER',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:divider(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSLCM',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:lcm(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSEQUAL',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:equal(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSMAX',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:max(${this.columns[0]}, ${this.columns[1]})`,
      },
      {
        name: 'CROSSMIN',
        validColTypes: NUMBER_TYPES,
        expression: () => `arithmetic:min(${this.columns[0]}, ${this.columns[1]})`,
      },
    ];

    this.SIMPLE_POPOVER_OPTIONS = [
      'SQUARE',
      'SQUAREROOT',
      'CUBE',
      'CUBEROOT',
      'LOG',
      'NATURALLOG',
      'ABSVALUE',
      'CEIL',
      'FLOOR',
      'SIN',
      'COS',
      'TAN',
      'ARCCOS',
      'ARCSIN',
      'ARCTAN',
      'ROUND',
      'RANDOM',
      'PRECISION',
      'SCALE',
      'UNSCALED',
      'NEGATE',
      'STRIPZERO',
      'SIGN',
      'DECIMALABSVALUE',
      'CROSSADD',
      'CROSSSUBTRACT',
      'CROSSMULTIPLY',
      'CROSSDIVIDEQ',
      'CROSSDIVIDER',
      'CROSSLCM',
      'CROSSEQUAL',
      'CROSSMAX',
      'CROSSMIN',
      'DECIMALSQUARE',
      'DECIMALCUBE',
    ];
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(
      this,
      document.getElementById('calculate-directive')
    );
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
      if (this.state.operationPopoverOpen) {
        let parentPopoverSelector = '#calculate-directive > .second-level-popover .option.active';
        let parentPopover = document.querySelector(parentPopover);
        if (!parentPopover) {
          parentPopoverSelector =
            '#calculate-directive > .second-level-popover .scrollable-list .option.active';
          parentPopover = document.querySelector(parentPopoverSelector);
        }
        if (parentPopover) {
          setPopoverOffset(parentPopover, 'third-level-popover');
        }
      } else {
        this.calculateOffset();
      }
    }
  }

  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  setDefaultState() {
    this.setState(this.defaultState);
  }

  setOperationInput(e) {
    let value = parseFloat(e.target.value, 10);
    if (typeof value !== 'number' || isNaN(value)) {
      return;
    }

    this.setState({
      operationInput: value,
    });
  }

  setNewColumnInput(e) {
    this.setState({
      newColumnInput: e.target.value,
    });
  }

  toggleCreateNewColumn() {
    this.setState({
      createNewColumn: !this.state.createNewColumn,
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
      newColumnInput,
    });

    this.setState(newState);
  }

  parseColumns = () => {
    let columns = typeof this.props.column === 'string' ? [this.props.column] : this.props.column;

    let type1 = DataPrepStore.getState().dataprep.typesCheck[columns[0]];
    if (columns.length === 1) {
      return {
        isDisabled: false,
        crossColumn: false,
        columns: columns[0],
        columnType: type1
      };
    }

    if (columns.length === 2) {
      const type2 = DataPrepStore.getState().dataprep.typesCheck[columns[1]];
      let index = columns.indexOf(this.props.ddSelected);
      if (type1 === type2 && index !== -1) {
        columns = index === 0 ? columns : [this.props.ddSelected, columns[0]];
        return {
          isDisabled: false,
          crossColumn: true,
          columns: columns,
          columnType: type1
        };
      }
    }

    type1 = DataPrepStore.getState().dataprep.typesCheck[columns];
    return {
      isDisabled: true,
      crossColumn: false,
      columns: columns,
      columnType: type1
    };
  }

  getCalculateOperations = () => {
    return this.crossColumn ? this.CROSS_COLUMN_CALCULATE_OPTIONS : this.CALCULATE_OPTIONS;
  }

  getExpressionAndApply() {
    if (this.isApplyDisabled()) {
      return;
    }

    let expression;
    let operationObj = this.getCalculateOperations().filter(
      (option) => this.state.operationPopoverOpen === option.name
    )[0];

    if (operationObj && typeof operationObj.expression === 'function') {
      expression = operationObj.expression();
    }

    this.applyDirective(expression);
  }

  isApplyDisabled() {
    return (
      ((this.state.createNewColumn || this.state.operationPopoverOpen === 'CHARCOUNT') &&
        this.state.newColumnInput.length === 0) ||
      (this.state.operationPopoverOpen === 'MODULO' && parseInt(this.state.operationInput, 10) < 1)
    );
  }

  applyDirective(expression) {
    let destinationColumn = this.props.ddSelected;
    if (
      (this.state.createNewColumn || this.state.operationPopoverOpen === 'CHARCOUNT') &&
      this.state.newColumnInput.length > 0
    ) {
      destinationColumn = this.state.newColumnInput;
    }
    let directive = `set-column ${destinationColumn} ${expression}`;

    execute([directive]).subscribe(
      () => {
        this.props.close();
        this.props.onComplete();
      },
      (err) => {
        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message,
          },
        });
      }
    );
  }

  renderOptions() {
    return this.getCalculateOperations().filter(
      (option) => option.name === 'label' || option.validColTypes.indexOf(this.columnType) !== -1
    ).map((option, i) => {
      const key = `${option.name}${i}`;
      if (option.name === 'label') {
        return (
          <div key={key} className="column-type-label">
            <span>
              {NUMBER_TYPES.indexOf(this.columnType) !== -1
                ? T.translate(`${PREFIX}.columnTypeLabel.numeric`)
                : capitalize(this.columnType)}
            </span>
          </div>
        );
      }
      if (option.name === 'divider') {
        return (
          <div key={key} className="column-action-divider calculate-options-divider">
            <hr />
          </div>
        );
      }
      return (
        <div
          key={key}
          className={classnames('option', {
            active: this.state.operationPopoverOpen === option.name,
          })}
          onClick={this.popoverOptionClick.bind(this, option.name)}
        >
          <span>{T.translate(`${PREFIX}.OptionsLabels.${option.name}`)}</span>
          <span className="float-right">
            <span className="fa fa-caret-right" />
          </span>
          {this.state.operationPopoverOpen === option.name ? this.renderOperationPopover() : null}
        </div>
      );
    });
  }

  renderOptionsWithScrollableList() {
    return <ScrollableList>{this.renderOptions()}</ScrollableList>;
  }

  renderDetail() {
    if (!this.props.isOpen || this.state.isDisabled) {
      return null;
    }

    return (
      <div className="second-level-popover" onClick={preventPropagation}>
        <div className="calculate-options">
          {/* Right now don't need to use ScrollableList for string options since there's only one of them.
            When we have more string options in the future, we can just do renderOptions() */

          NUMBER_TYPES.indexOf(this.columnType) !== -1
            ? this.renderOptionsWithScrollableList()
            : this.renderOptions()}
        </div>
      </div>
    );
  }

  renderNewColumnNameInputWithCheckbox() {
    return (
      <div>
        <div className="create-new-column-line" onClick={this.toggleCreateNewColumn}>
          <span className="fa fa-fw">
            <IconSVG name={this.state.createNewColumn ? 'icon-check-square' : 'icon-square-o'} />
          </span>

          <span>{T.translate(`${COPY_NEW_COLUMN_PREFIX}.label`)}</span>
        </div>
        {this.renderNewColumnNameInput()}
      </div>
    );
  }

  renderNewColumnNameInput() {
    if (!this.state.createNewColumn && this.state.operationPopoverOpen !== 'CHARCOUNT') {
      return null;
    }

    let inputLabel = T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputLabel`);
    let inputPlaceholder = T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputPlaceholder`);

    if (this.state.operationPopoverOpen === 'CHARCOUNT') {
      inputLabel = T.translate(`${PREFIX}.destinationColumnInputLabel`);
    }

    return (
      <div>
        <div
          className={classnames({
            'bigger-new-column-label': this.state.operationPopoverOpen === 'CHARCOUNT',
          })}
        >
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
        {columnNameAlreadyExists(this.state.newColumnInput) ? (
          <WarningContainer message={T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputDuplicate`)} />
        ) : null}
      </div>
    );
  }

  renderActionButtons() {
    return (
      <div className="action-buttons">
        <button
          className="btn btn-primary float-left"
          disabled={this.isApplyDisabled()}
          onClick={this.getExpressionAndApply}
        >
          {T.translate('features.DataPrep.Directives.apply')}
        </button>

        <button className="btn btn-link float-right" onClick={this.setDefaultState}>
          {T.translate('features.DataPrep.Directives.cancel')}
        </button>
      </div>
    );
  }

  renderOperationPopover() {
    if (!this.state.operationPopoverOpen) {
      return null;
    }

    if (this.state.operationPopoverOpen === 'CHARCOUNT') {
      return (
        <div className="third-level-popover" onClick={preventPropagation}>
          {this.renderNewColumnNameInput()}
          <hr />
          {this.renderActionButtons()}
        </div>
      );
    }

    if (this.SIMPLE_POPOVER_OPTIONS.indexOf(this.state.operationPopoverOpen) !== -1) {
      return (
        <div className="third-level-popover" onClick={preventPropagation}>
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
      <div className="third-level-popover" onClick={preventPropagation}>
        {this.state.operationPopoverOpen === 'POWEROF' ? (
          <div className="operation-description">
            {T.translate(`${PREFIX}.Options.POWEROF.description`)}
          </div>
        ) : null}
        <div className="operation-sign-input">
          <span className="operation-sign">{operationSign}</span>
          <Input
            className="operation-input mousetrap"
            value={this.state.operationInput}
            onChange={this.setOperationInput}
            type="number"
            step="any"
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
            active: this.props.isOpen && !this.state.isDisabled,
            disabled: this.state.isDisabled,
          })}
        >
          <span>{T.translate(`${PREFIX}.title`)}</span>

          <span className="float-right">
            <span className="fa fa-caret-right" />
          </span>

          {this.renderDetail()}
        </div>
        {this.state.isDisabled ? (
          <UncontrolledTooltip target={id} delay={{ show: 250, hide: 0 }}>
            {T.translate(`${PREFIX}.disabledTooltip`)}
          </UncontrolledTooltip>
        ) : null}
      </div>
    );
  }
}
Calculate.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.oneOfType([PropTypes.array, PropTypes.string]),
  close: PropTypes.func,
  onComplete: PropTypes.func,
};
