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
import {isCustomOption} from 'components/DataPrep/helper';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import WarningContainer from 'components/WarningContainer';
import T from 'i18n-react';
require('./MergeColumns.scss');

const PREFIX = `features.DataPrep.Directives.Merge`;
const DEFAULT_DELIMITER = 'COMMA';
const DELIMITER_MAP = {
  'COMMA': ',',
  'PIPE': '|',
  'COLON': ':',
  'PERIOD': '.',
  'DASH': '-',
  'UNDERSCORE': '_'
};

export default class MergeColumnsDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      firstColumn: this.props.column[0],
      secondColumn: this.props.column[1],
      newColumnInput: `${this.props.column[0]}_${this.props.column[1]}`,
      selectedDelimiter: DEFAULT_DELIMITER,
      customDelimiter: '',
      isOpen: this.props.isOpen
    };

    this.handleNewColumnInputChange = this.handleNewColumnInputChange.bind(this);
    this.handleDelimiterSelect = this.handleDelimiterSelect.bind(this);
    this.handleCustomDelimiterChange = this.handleCustomDelimiterChange.bind(this);
    this.switchColumnOrder = this.switchColumnOrder.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.isOpen !== this.state.isOpen) {
      this.setState({
        isOpen: nextProps.isOpen
      });
    }
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  handleNewColumnInputChange(e) {
    this.setState({newColumnInput: e.target.value});
  }

  handleDelimiterSelect(e) {
    this.setState({selectedDelimiter: e.target.value});
  }

  handleCustomDelimiterChange(e) {
    this.setState({customDelimiter: e.target.value});
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode === 13) {
      this.applyDirective();
      this.preventPropagation(e);
    }
  }

  columnAlreadyExists(colName) {
    let headers = DataPrepStore.getState().dataprep.headers;
    return headers.indexOf(colName) !== -1;
  }

  switchColumnOrder() {
    let switchedState = {
      firstColumn: this.state.secondColumn,
      secondColumn: this.state.firstColumn
    };
    if (this.state.newColumnInput === `${this.state.firstColumn}_${this.state.secondColumn}`) {
      switchedState = Object.assign({}, switchedState, {
        newColumnInput: `${this.state.secondColumn}_${this.state.firstColumn}`
      });
    }
    this.setState(switchedState);
  }

  applyDirective() {
    let newColumnInput = this.state.newColumnInput;
    let delimiterSymbol = DELIMITER_MAP[this.state.selectedDelimiter];

    if (isCustomOption(this.state.selectedDelimiter)) {
      delimiterSymbol = this.state.customDelimiter;
    }

    let directive = `merge ${this.state.firstColumn} ${this.state.secondColumn} ${newColumnInput} ${delimiterSymbol}`;

    execute([directive])
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

  renderCustomDelimiter() {
    if (!isCustomOption(this.state.selectedDelimiter)) { return null; }

    return (
      <div>
        <br />
        <textarea
          className="form-control"
          value={this.state.customDelimiter}
          onChange={this.handleCustomDelimiterChange}
          autoFocus
          placeholder={T.translate(`${PREFIX}.customDelimiterPlaceholder`)}
        />
      </div>
    );
  }

  renderDetail() {
    if (!this.state.isOpen) { return null; }
    let disabledCondition = this.state.newColumnInput.length === 0
    || (isCustomOption(this.state.selectedDelimiter)
    && this.state.customDelimiter.length === 0);

    let mergeDelimiters = Object.keys(DELIMITER_MAP).map((delimiter) => {
      return {
        delimiter: delimiter,
        displayText: T.translate(`${PREFIX}.Delimiters.${delimiter}`)
      };
    });

    return (
      <div
        className="merge-columns-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        <h5 className="set-order">
          {T.translate(`${PREFIX}.setOrder`)}
        </h5>

        <div className="columns-order">
          <span className="columns-names">
            <span>{this.state.firstColumn}</span>
            <hr />
            <span>{this.state.secondColumn}</span>
          </span>
          <span
            className="fa fa-exchange"
            onClick={this.switchColumnOrder}
          />
        </div>

        <br />

        <h5>{T.translate(`${PREFIX}.chooseDelimiter`)}</h5>

        <div>
          <select
            className="form-control mousetrap"
            value={this.state.selectedDelimiter}
            onChange={this.handleDelimiterSelect}
          >
            {
              mergeDelimiters.map((delimiterOption) => {
                return (
                  <option
                    value={delimiterOption.delimiter}
                    key={delimiterOption.delimiter}
                  >
                    {delimiterOption.displayText}
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
            <option value="CUSTOMDELIMITER">
              {T.translate(`${PREFIX}.Delimiters.CUSTOMDELIMITER`)}
            </option>
          </select>
        </div>

        {this.renderCustomDelimiter()}

        <br />

        <h5>{T.translate(`${PREFIX}.newColumn`)}</h5>

        <div className="input">
          <input
            type="text"
            className="form-control mousetrap"
            value={this.state.newColumnInput}
            onKeyPress={this.handleKeyPress}
            onChange={this.handleNewColumnInputChange}
            placeholder={T.translate(`${PREFIX}.newColumnPlaceholder`)}
            autoFocus
          />
        </div>

        {
          this.columnAlreadyExists(this.state.newColumnInput) ? (
            <WarningContainer
              message={T.translate(`${PREFIX}.duplicate`)}
            />
          ) : null
        }

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={disabledCondition}
          >
            {T.translate(`${PREFIX}.buttonLabel`)}
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
        className={classnames('merge-columns-directive clearfix action-item', {
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

MergeColumnsDirective.propTypes = {
  column: PropTypes.array,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
