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
import SimpleDateModal from 'components/DataPrep/Directives/Parse/Modals/SimpleDateModal';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import IconSVG from 'components/IconSVG';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import WarningContainer from 'components/WarningContainer';
import {preventPropagation} from 'services/helpers';
import {columnNameAlreadyExists} from 'components/DataPrep/helper';
import capitalize from 'lodash/capitalize';
import Mousetrap from 'mousetrap';

const PREFIX = 'features.DataPrep.Directives.Format';
const COPY_NEW_COLUMN_PREFIX = 'features.DataPrep.DataPrepTable.copyToNewColumn';
const VALID_TYPES = ['string', 'date'];

export default class Format extends Component {
  columnType = DataPrepStore.getState().dataprep.types[this.props.column];

  defaultFormatPopoverState = {
    formatPopoverOpen: null,
    formatInput: '',
    createNewColumn: false,
    newColumnInput: this.props.column + T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputSuffix`),
    concatOption: 'BEGINNING'
  }

  state = Object.assign({}, this.defaultFormatPopoverState, {
    activeModal: null,
    isDisabled: VALID_TYPES.indexOf(this.columnType) === -1
  })

  applyDirective = (directive) => {
    if (this.isApplyDisabled()) {
      return;
    }

    execute([directive])
      .subscribe(
        this.toggleModal,
        (err) => {
          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  };

  popoverOptionClick = (option) => {
    let formatPopoverOpen = option === this.state.formatPopoverOpen ? null : option;

    this.setState({ formatPopoverOpen });
  };

  formatToDateTime = () => {
    this.setState({
      activeModal: (
        <SimpleDateModal
          source="format"
          toggle={this.toggleModal}
          onApply={this.applyDateFormat}
        />
      )
    });
  };

  FORMAT_OPTIONS = [
    {
      name: 'DATE_TIME',
      onClick: this.formatToDateTime,
      validColTypes: ['date']
    },
    {
      name: 'UPPERCASE',
      onClick: this.applyDirective.bind(this, `uppercase ${this.props.column}`),
      validColTypes: ['string']
    },
    {
      name: 'LOWERCASE',
      onClick: this.applyDirective.bind(this, `lowercase ${this.props.column}`),
      validColTypes: ['string']
    },
    {
      name: 'TITLECASE',
      onClick: this.applyDirective.bind(this, `titlecase ${this.props.column}`),
      validColTypes: ['string']
    },
    {
      name: 'CONCATENATE',
      onClick: this.popoverOptionClick.bind(this, 'CONCATENATE'),
      validColTypes: ['string']
    },
    {
      name: 'TRIM_WHITESPACE',
      onClick: this.applyDirective.bind(this, `trim ${this.props.column}`),
      validColTypes: ['string']
    },
    {
      name: 'TRIM_LEADING_WHITESPACE',
      onClick: this.applyDirective.bind(this, `ltrim ${this.props.column}`),
      validColTypes: ['string']
    },
    {
      name: 'TRIM_TRAILING_WHITESPACE',
      onClick: this.applyDirective.bind(this, `rtrim ${this.props.column}`),
      validColTypes: ['string']
    }
  ];

  CONCATENATE_OPTIONS = [
    {
      name: 'BEGINNING',
      label: T.translate(`${PREFIX}.Formats.CONCATENATE.addOptions.BEGINNING`)
    },
    {
      name: 'END',
      label: T.translate(`${PREFIX}.Formats.CONCATENATE.addOptions.END`)
    }
  ];

  componentWillReceiveProps(nextProps) {
    if (!nextProps.isOpen) {
      this.setDefaultFormatPopoverState();
    }
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('format-directive'));
  }

  componentDidUpdate() {
    if (this.props.isOpen) {
      Mousetrap.bind('enter', this.getConcatExpressionAndApply);
    }
    if (this.props.isOpen && !this.state.isDisabled && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  toggleModal = () => {
    this.setState({
      activeModal: null
    });
  };

  toggleCreateNewColumn = () => {
    this.setState({
      createNewColumn: !this.state.createNewColumn
    });
  };

  setConcatOption = (e) => {
    this.setState({
      concatOption: e.target.value
    });
  };

  setFormatInput = (e) => {
    this.setState({
      formatInput: e.target.value
    });
  };

  setNewColumnInput = (e) => {
    this.setState({
      newColumnInput: e.target.value
    });
  };

  applyDateFormat = (name, format) => {
    let directive = `format-date ${this.props.column} ${format}`;
    this.applyDirective(directive);
  };

  setDefaultFormatPopoverState = () => {
    this.setState(this.defaultFormatPopoverState);
  };

  getConcatExpressionAndApply = () => {
    let destinationColumn = this.props.column;
    if (this.state.createNewColumn && this.state.newColumnInput.length > 0) {
      destinationColumn = this.state.newColumnInput;
    }
    let expression;
    if (this.state.concatOption === 'BEGINNING') {
      expression = `'${this.state.formatInput}' + ${this.props.column}`;
    } else {
      expression = `${this.props.column} + '${this.state.formatInput}'`;
    }
    let directive = `set-column ${destinationColumn} ${expression}`;

    this.applyDirective(directive);
  };

  isApplyDisabled() {
    return (this.state.formatPopoverOpen === 'CONCATENATE' && this.state.formatInput.length === 0) ||
    (this.state.createNewColumn && this.state.newColumnInput.length === 0);
  }

  renderActionButtons() {
    return (
      <div className="action-buttons">
        <button
          className="btn btn-primary float-xs-left"
          disabled={this.isApplyDisabled()}
          onClick={this.getConcatExpressionAndApply}
        >
          {T.translate('features.DataPrep.Directives.apply')}
        </button>

        <button
          className="btn btn-link float-xs-right"
          onClick={this.setDefaultFormatPopoverState}
        >
          {T.translate('features.DataPrep.Directives.cancel')}
        </button>
      </div>
    );
  }

  renderNewColumnNameInput() {
    if (!this.state.createNewColumn) { return null; }

    return (
      <div>
        <Input
          type="text"
          className="form-control mousetrap"
          value={this.state.newColumnInput}
          onChange={this.setNewColumnInput}
          placeholder={T.translate(`${COPY_NEW_COLUMN_PREFIX}.inputPlaceholder`)}
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

  renderFormatPopover() {
    if (!this.state.formatPopoverOpen) { return null; }

    return (
      <div
        className="third-level-popover"
        onClick={preventPropagation}
      >
        <div>
          {T.translate(`${PREFIX}.Formats.CONCATENATE.addLabel`)}
        </div>
        <Input
          className="format-input mousetrap"
          value={this.state.formatInput}
          placeholder={T.translate(`${PREFIX}.Formats.CONCATENATE.inputPlaceholder`)}
          onChange={this.setFormatInput}
          autoFocus
        />
        <Input
          type="select"
          className="concatenate-option-select"
          onChange={this.setConcatOption}
          value={this.state.concatOption}
        >
          {
            this.CONCATENATE_OPTIONS.map(option => {
              return (
                <option value={option.name}>
                  {option.label}
                </option>
              );
            })
          }
        </Input>
        <div>
          {T.translate(`${PREFIX}.Formats.CONCATENATE.addDescription`)}
        </div>

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

        <hr />

        {this.renderActionButtons()}
      </div>
    );
  }

  renderModal() {
    return this.state.activeModal;
  }
  renderOptions() {
    return (
      this.FORMAT_OPTIONS
      .filter(option => option.validColTypes.indexOf(this.columnType) !== -1)
      .map((option) => {
        return (
          <div
            key={option.name}
            className={classnames('option', {
              'active': this.state.formatPopoverOpen === option.name
            })}
            onClick={option.onClick}
          >
            {T.translate(`${PREFIX}.Formats.${option.name}.label`)}
            {
              option.name === 'CONCATENATE' ?
                (
                  <span className="float-xs-right">
                    <IconSVG name="icon-caret-right" />
                  </span>
                )
              : null
            }
            {
              this.state.formatPopoverOpen === option.name ?
                this.renderFormatPopover()
              : null
            }
          </div>
        );
      })
    );
  }
  renderDetail() {
    if (!this.props.isOpen || this.state.isDisabled) { return null; }

    return (
      <div
        className="parse-detail second-level-popover"
        onClick={preventPropagation}
      >
        <div className="column-type-label">
          <span>
            {capitalize(this.columnType)}
          </span>
        </div>
        <div className="parse-options">
          {this.renderOptions()}
        </div>
      </div>
    );
  }
  render() {
    let id = 'format-directive';

    return (
      <div>
        <div
          id={id}
          className={classnames('parse-directive clearfix action-item', {
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
          {this.renderModal()}
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
Format.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.string
};
