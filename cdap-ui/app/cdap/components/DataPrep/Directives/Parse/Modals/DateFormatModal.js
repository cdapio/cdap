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
  OPTION1: {
    format: 'MM/dd/yyyy',
    hasDate: true,
    hasTime: false,
  },
  OPTION2: {
    format: 'dd/MM/yyyy',
    hasDate: true,
    hasTime: false,
  },
  OPTION3: {
    format: 'MM-dd-yyyy',
    hasDate: true,
    hasTime: false,
  },
  OPTION4: {
    format: 'MM-dd-yy',
    hasDate: true,
    hasTime: false,
  },
  OPTION5: {
    format: 'yyyy-MM-dd',
    hasDate: true,
    hasTime: false,
  },
  OPTION6: {
    format: 'yyyy-MM-dd HH:mm:ss',
    hasDate: true,
    hasTime: true,
  },
  OPTION7: {
    format: "MM-dd-yyyy 'at' HH:mm:ss z",
    hasDate: true,
    hasTime: true,
  },
  OPTION8: {
    format: 'dd/MM/yy HH:mm:ss',
    hasDate: true,
    hasTime: true,
  },
  OPTION9: {
    format: "yyyy,MM.dd'T'HH:mm:ss.SSSZ",
    hasDate: true,
    hasTime: true,
  },
  OPTION10: {
    format: 'MM.dd.yyyy HH:mm:ss.SSS',
    hasDate: true,
    hasTime: true,
  },
  OPTION11: {
    format: 'EEE, d MMM yyyy HH:mm:ss',
    hasDate: true,
    hasTime: true,
  },
  OPTION12: {
    format: "EEE, MMM d, ''yy",
    hasDate: true,
    hasTime: false,
  },
  OPTION13: {
    format: 'h:mm a',
    hasDate: false,
    hasTime: true,
  },
  OPTION14: {
    format: 'H:mm a, z',
    hasDate: false,
    hasTime: true,
  },
  CUSTOM: {
    format: 'CUSTOM',
    hasDate: true,
    hasTime: true,
  }
};

export default class DateFormatModal extends Component {
  constructor(props) {
    super(props);

    const options = Object.keys(OPTIONS_MAP).filter(option => {
      const optionObject = OPTIONS_MAP[option];
      if (props.mustHaveDate && !optionObject.hasDate) {
        return false;
      }
      if (props.mustHaveTime && !optionObject.hasTime) {
        return false;
      }
      return true;
    });

    this.state = {
      format: options[0],
      customFormat: '',
      options,
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
    let { format } = OPTIONS_MAP[this.state.format];

    if (this.state.format === 'CUSTOM') {
      format = this.state.customFormat;
    }

    this.props.onApply(format);
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
          placeholder={T.translate(`${PREFIX}.Parsers.SIMPLEDATE.customPlaceholder`)}
          autoFocus
        />
      </div>
    );
  }

  render() {
    const disabled = this.state.format === 'CUSTOM' && this.state.customFormat.length === 0;

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
          <span>
            {T.translate(`${PREFIX}.Parsers.SIMPLEDATE.ModalHeader.${this.props.source}`, {
              parser: this.props.parserName,
            })}
          </span>

          <div className="close-section float-right" onClick={this.props.toggle}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <h5>{T.translate(`${PREFIX}.Parsers.SIMPLEDATE.modalTitle`)}</h5>

          <br />

          <div className="list-options">
            {this.state.options.map((option) => {
              return (
                <div key={option} onClick={this.selectFormat.bind(this, option)}>
                  <span
                    className={classnames('fa', {
                      'fa-circle-o': option !== this.state.format,
                      'fa-circle': option === this.state.format,
                    })}
                  />
                  <span>{T.translate(`${PREFIX}.Parsers.SIMPLEDATE.Options.${option}`)}</span>
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

DateFormatModal.propTypes = {
  source: PropTypes.string,
  toggle: PropTypes.func,
  parserName: PropTypes.string,
  mustHaveDate: PropTypes.bool,
  mustHaveTime: PropTypes.bool,
  onApply: PropTypes.func,
};
