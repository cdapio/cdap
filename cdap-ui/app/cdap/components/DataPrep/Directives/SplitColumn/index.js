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

export default class SplitColumnDirective extends Component {
  constructor(props) {
    super(props);

    const DEFAULT_DELIMITER = 'Comma';

    this.state = {
      delimiterSelection: DEFAULT_DELIMITER,
      customDelimiter: ''
    };

    this.applyDirective = this.applyDirective.bind(this);
    this.handleDelimiterInput = this.handleDelimiterInput.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.state.delimiterSelection === 'Custom Separator' && this.state.customDelimiter.length === 0) {
      this.customDelimiterRef.focus();
    }
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  handleSplitByClick(option) {
    this.setState({delimiterSelection: option});
  }

  handleDelimiterInput(e) {
    this.setState({customDelimiter: e.target.value});
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode !== 13 || this.state.customDelimiter.length === 0) { return; }

    this.applyDirective();
  }

  applyDirective() {
    const DELIMITER_MAP = {
      'Comma': ',',
      'Tab': '\\t',
      'Pipe': '\\|',
      'Whitespace': '\\s+',
      'Custom Separator': 'CUSTOM'
    };

    let delimiter = DELIMITER_MAP[this.state.delimiterSelection];
    let column = this.props.column;

    if (delimiter === 'CUSTOM') {
      if (this.state.customDelimiter.length === 0) { return; }

      delimiter = this.state.customDelimiter;
    }

    let directive = `split-to-columns ${column} ${delimiter}`;

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

  renderCustomDelimiter() {
    if (this.state.delimiterSelection !== 'Custom Separator') { return null; }

    return (
      <div className="custom-delimiter-input">
        <input
          type="text"
          className="form-control"
          onChange={this.handleDelimiterInput}
          onKeyPress={this.handleKeyPress}
          value={this.state.customDelimiter}
          placeholder="e.g. \d+"
          ref={ref => this.customDelimiterRef = ref}
        />
      </div>
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    const OPTIONS = [
      'Comma',
      'Tab',
      'Pipe',
      'Whitespace',
      'Custom Separator'
    ];

    let isOKdisabled = this.state.delimiterSelection === 'Custom Separator' && this.state.customDelimiter.length === 0;

    return (
      <div
        className="split-column-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        <h5>Split On</h5>

        {
          OPTIONS.map((option) => {
            return (
              <div
                key={option}
                onClick={this.handleSplitByClick.bind(this, option)}
              >
                <span className={classnames('fa fa-fw', {
                  'fa-circle-o': option !== this.state.delimiterSelection,
                  'fa-circle': option === this.state.delimiterSelection
                })} />
                <span>{option}</span>
              </div>
            );
          })
        }

        {this.renderCustomDelimiter()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={isOKdisabled}
          >
            Apply
          </button>

          <button
            className="btn btn-link float-xs-right"
            onClick={this.props.close}
          >
            Cancel
          </button>
        </div>

      </div>
    );
  }

  render() {
    return (
      <div
        className={classnames('split-column-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>Split to Columns (Regex)</span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

SplitColumnDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};

