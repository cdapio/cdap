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
  COMMON: 'common',
  COMBINED: 'combined',
  COMBINEDIO: 'combinedio',
  REFERER: 'referer',
  AGENT: 'agent',
  CUSTOM: 'CUSTOM',
};

export default class LogModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      format: 'COMMON',
      customFormat: '',
    };

    this.apply = this.apply.bind(this);
    this.handleCustomFormatChange = this.handleCustomFormatChange.bind(this);
  }

  componentDidMount() {
    MouseTrap.bind('enter', this.apply);
  }

  componentWillUnmount() {
    MouseTrap.unbind('enter');
  }

  apply() {
    let format = OPTIONS_MAP[this.state.format];

    if (this.state.format === 'CUSTOM') {
      format = this.state.customFormat;
    }

    this.props.onApply('LOG', `'${format}'`);
    this.props.toggle();
  }

  selectFormat(option) {
    this.setState({ format: option });
  }

  handleCustomFormatChange(e) {
    this.setState({ customFormat: e.target.value });
  }

  renderCustomText() {
    if (this.state.format !== 'CUSTOM') {
      return null;
    }

    return (
      <div className="custom-format">
        <input
          type="text"
          className="form-control mousetrap"
          value={this.state.customFormat}
          onChange={this.handleCustomFormatChange}
          placeholder={T.translate(`${PREFIX}.Parsers.LOG.customPlaceholder`)}
          autoFocus
        />
      </div>
    );
  }

  render() {
    let options = Object.keys(OPTIONS_MAP);

    let disabled = this.state.format === 'CUSTOM' && this.state.customFormat.length === 0;

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal cdap-modal"
      >
        <ModalHeader>
          <span>{T.translate(`${PREFIX}.modalTitle`, { parser: 'Log' })}</span>

          <div className="close-section float-right" onClick={this.props.toggle}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <h5>{T.translate(`${PREFIX}.Parsers.LOG.modalTitle`)}</h5>

          <br />

          <div className="list-options">
            {options.map((option) => {
              return (
                <div key={option} onClick={this.selectFormat.bind(this, option)}>
                  <span
                    className={classnames('fa', {
                      'fa-circle-o': option !== this.state.format,
                      'fa-circle': option === this.state.format,
                    })}
                  />
                  <span>{T.translate(`${PREFIX}.Parsers.LOG.Options.${option}`)}</span>
                </div>
              );
            })}
          </div>

          {this.renderCustomText()}
        </ModalBody>

        <ModalFooter>
          <button className="btn btn-primary" onClick={this.apply} disabled={disabled}>
            {T.translate('features.DataPrep.Directives.apply')}
          </button>
          <button className="btn btn-secondary" onClick={this.props.toggle}>
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }
}

LogModal.propTypes = {
  toggle: PropTypes.func,
  onApply: PropTypes.func,
};
