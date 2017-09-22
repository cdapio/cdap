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
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import classnames from 'classnames';
import T from 'i18n-react';
import MouseTrap from 'mousetrap';

const PREFIX = 'features.DataPrep.Directives.Parse';

const OPTIONS_MAP = {
  'COMMA': ',',
  'TAB': '\\t',
  'SPACE': ' ',
  'PIPE': '\\|',
  'CONTROL_A': '\\u0001',
  'CONTROL_D': '\\u0004',
  'CUSTOM': 'CUSTOM'
};

export default class CSVModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      delimiter: 'COMMA',
      firstRowHeader: false,
      customDelimiter: ''
    };

    this.apply = this.apply.bind(this);
    this.toggleSetFirstRow = this.toggleSetFirstRow.bind(this);
    this.handleCustomDelimiterChange = this.handleCustomDelimiterChange.bind(this);
  }

  componentDidMount() {
    MouseTrap.bind('enter', this.apply);
  }

  componentWillUnmount() {
    MouseTrap.unbind('enter');
  }

  apply() {
    let delimiter = OPTIONS_MAP[this.state.delimiter];

    if (this.state.delimiter === 'CUSTOM') {
      delimiter = this.state.customDelimiter;
    }

    let configuration = `'${delimiter}' ${this.state.firstRowHeader}`;

    this.props.onApply('CSV', configuration);
    this.props.toggle();
  }

  selectDelimiter(option) {
    this.setState({delimiter: option});
  }

  toggleSetFirstRow() {
    this.setState({firstRowHeader: !this.state.firstRowHeader});
  }

  handleCustomDelimiterChange(e) {
    this.setState({customDelimiter: e.target.value});
  }

  renderCustomText() {
    if (this.state.delimiter !== 'CUSTOM') { return null; }

    return (
      <div className="custom-format">
        <input
          type="text"
          className="form-control mousetrap"
          value={this.state.customDelimiter}
          onChange={this.handleCustomDelimiterChange}
          placeholder={T.translate(`${PREFIX}.Parsers.CSV.customPlaceholder`)}
          autoFocus
        />
      </div>
    );
  }

  render() {
    let options = Object.keys(OPTIONS_MAP);

    let disabled = this.state.delimiter === 'CUSTOM' && this.state.customDelimiter.length === 0;

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal"
      >
        <ModalHeader>
          <span>
            {T.translate(`${PREFIX}.modalTitle`, {parser: 'CSV'})}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <h5>
            {T.translate(`${PREFIX}.Parsers.CSV.modalTitle`)}
          </h5>

          <br />

          <div className="list-options">
            {
              options.map((option) => {
                return (
                  <div
                    key={option}
                    onClick={this.selectDelimiter.bind(this, option)}
                  >
                    <span
                      className={classnames('fa', {
                        'fa-circle-o': option !== this.state.delimiter,
                        'fa-circle': option === this.state.delimiter
                      })}
                    />
                    <span>
                      {T.translate(`${PREFIX}.Parsers.CSV.Options.${option}`)}
                    </span>
                  </div>
                );
              })
            }

            {this.renderCustomText()}
          </div>
          <div className="optional-config">
            <span
              onClick={this.toggleSetFirstRow}
            >
              <span
                className={classnames('fa', {
                  'fa-square-o': !this.state.firstRowHeader,
                  'fa-check-square': this.state.firstRowHeader
                })}
              />
              <span>
                {T.translate(`${PREFIX}.Parsers.CSV.firstRowHeader`)}
              </span>
            </span>
          </div>
        </ModalBody>

        <ModalFooter>
          <button
            className="btn btn-primary"
            onClick={this.apply}
            disabled={disabled}
          >
            {T.translate('features.DataPrep.Directives.apply')}
          </button>
          <button
            className="btn btn-secondary"
            onClick={this.props.toggle}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }
}


CSVModal.propTypes = {
  toggle: PropTypes.func,
  onApply: PropTypes.func,
};
