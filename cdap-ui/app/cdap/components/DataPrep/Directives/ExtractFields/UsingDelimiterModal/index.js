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
import {Modal, ModalHeader, ModalBody, ModalFooter} from 'reactstrap';
import classnames from 'classnames';
import T from 'i18n-react';
import Mousetrap from 'mousetrap';
import isEmpty from 'lodash/isEmpty';
require('./UsingDelimiter.scss');

const PREFIX = 'features.DataPrep.Directives.ExtractFields.UsingDelimiters';
const DEFAULT_DELIMITER = 'Comma';
const DELIMITER_MAP = {
  'Comma': ',',
  'Tab': '\\t',
  'Pipe': '\\|',
  'Whitespace': '\\s+',
  'Custom Separator': 'CUSTOM'
};

export default class UsingDelimiterModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      delimiterSelection: DEFAULT_DELIMITER,
      customDelimiter: ''
    };
    this.applyDirective = this.applyDirective.bind(this);
    this.handleDelimiterInput = this.handleDelimiterInput.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }
  componentDidMount() {
    Mousetrap.bind('enter', this.applyDirective);
  }
  componentWillUnmount() {
    Mousetrap.unbind('enter');
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
  renderCustomDelimiter() {
    if (this.state.delimiterSelection !== 'Custom Separator') { return null; }

    return (
      <div className="custom-delimiter-input">
        <input
          type="text"
          className="form-control mousetrap"
          onChange={this.handleDelimiterInput}
          onKeyPress={this.handleKeyPress}
          value={this.state.customDelimiter}
          placeholder="e.g. \d+"
          autoFocus
        />
      </div>
    );
  }
  applyDirective() {

    let delimiter = DELIMITER_MAP[this.state.delimiterSelection];

    if (delimiter === 'CUSTOM') {
      if (this.state.customDelimiter.length === 0) { return; }

      delimiter = this.state.customDelimiter;
    }

    if (this.props.onApply) {
      this.props.onApply(delimiter);
    }
  }

  render() {
    const OPTIONS = Object.keys(DELIMITER_MAP);
    const isCustomDelimiter = () => DELIMITER_MAP[this.state.delimiterSelection] === 'CUSTOM';
    const getApplyBtnDisabledState = () => isCustomDelimiter() && isEmpty(this.state.customDelimiter);
    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal using-delimiter-modal"
      >
        <ModalHeader>

          <span>
            {T.translate(`${PREFIX}.modalTitle`)}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.onClose}
          >
            <span className="fa fa-times" />
          </div>

        </ModalHeader>
        <ModalBody>
          {
            OPTIONS.map((option) => {
              return (
                <div
                  key={option}
                  onClick={this.handleSplitByClick.bind(this, option)}
                  className="cursor-pointer"
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
        </ModalBody>
        <ModalFooter>
          <button
            className="btn btn-primary"
            onClick={this.applyDirective}
            disabled={getApplyBtnDisabledState()}
          >
            {T.translate('features.DataPrep.Directives.ExtractFields.extractBtnLabel')}
          </button>
          <button
            className="btn btn-secondary"
            onClick={this.props.onClose}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }
}

UsingDelimiterModal.propTypes = {
  onClose: PropTypes.func,
  onApply: PropTypes.func
};
