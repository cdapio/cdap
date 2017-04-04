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
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import classnames from 'classnames';
import T from 'i18n-react';

const SUFFIX = 'features.DataPrep.Directives.Parse';

const OPTIONS_MAP = {
  'COMMA': ',',
  'TAB': '\\t',
  'PIPE': '\\|'
};

export default class CSVModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      delimiter: 'COMMA',
      shouldSkip: false
    };

    this.apply = this.apply.bind(this);
    this.toggleShouldSkip = this.toggleShouldSkip.bind(this);
  }


  apply() {
    let delimiter = OPTIONS_MAP[this.state.delimiter];
    let configuration = `${delimiter} ${this.state.shouldSkip}`;

    this.props.onApply('CSV', configuration);
    this.props.toggle();
  }

  selectDelimiter(option) {
    this.setState({delimiter: option});
  }

  toggleShouldSkip() {
    this.setState({shouldSkip: !this.state.shouldSkip});
  }

  render() {
    let options = Object.keys(OPTIONS_MAP);

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="md"
        backdrop="static"
        className="dataprep-parse-modal"
      >
        <ModalHeader>
          <span>
            {T.translate(`${SUFFIX}.modalTitle`, {parser: 'CSV'})}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
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
                      {T.translate(`${SUFFIX}.Parsers.CSV.Options.${option}`)}
                    </span>
                  </div>
                );
              })
            }
          </div>
          <div className="optional-config">
            <span
              onClick={this.toggleShouldSkip}
            >
              <span
                className={classnames('fa', {
                  'fa-square-o': !this.state.shouldSkip,
                  'fa-check-square': this.state.shouldSkip
                })}
              />
              <span>
                {T.translate(`${SUFFIX}.Parsers.CSV.shouldSkipLabel`)}
              </span>
            </span>
          </div>
        </ModalBody>

        <ModalFooter>
          <button
            className="btn btn-primary"
            onClick={this.apply}
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
