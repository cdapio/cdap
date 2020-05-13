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
import T from 'i18n-react';
import MouseTrap from 'mousetrap';
const SUFFIX = 'features.DataPrep.Directives.Parse';

export default class SingleFieldModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      text: this.props.defaultValue || '',
      optionalText: '',
    };

    this.onTextChange = this.onTextChange.bind(this);
    this.onOptionalTextChange = this.onOptionalTextChange.bind(this);
    this.apply = this.apply.bind(this);
  }

  componentDidMount() {
    MouseTrap.bind('enter', this.apply);
  }

  componentWillUnmount() {
    MouseTrap.reset();
  }

  onTextChange(e) {
    this.setState({ text: e.target.value });
  }

  onOptionalTextChange(e) {
    this.setState({ optionalText: e.target.value });
  }

  apply() {
    if (!this.state.text) {
      return false;
    }
    let configuration = this.state.text;

    if (this.props.hasOptionalField && this.state.optionalText.length > 0) {
      configuration = `${this.state.text} ${this.state.optionalText}`;
    }

    this.props.onApply(this.props.parser, configuration);
    this.props.toggle();
  }

  renderOptionalField() {
    if (!this.props.hasOptionalField) {
      return null;
    }
    let parser = this.props.parser;

    return (
      <div>
        <br />
        <label className="control-label">
          {T.translate(`${SUFFIX}.Parsers.${parser}.optionalFieldLabel`)}
        </label>
        <input
          type="text"
          className="form-control mousetrap"
          placeholder={T.translate(`${SUFFIX}.Parsers.${parser}.optionalPlaceholder`)}
          value={this.state.optionalText}
          onChange={this.onOptionalTextChange}
        />
      </div>
    );
  }

  render() {
    let parser = this.props.parser;
    let disabled = this.props.isTextRequired && this.state.text.length === 0;

    let parserTitle = T.translate(`${SUFFIX}.Parsers.${parser}.label`);

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal cdap-modal"
        autoFocus={false}
      >
        <ModalHeader>
          <span>{T.translate(`${SUFFIX}.modalTitle`, { parser: parserTitle })}</span>

          <div className="close-section float-right" onClick={this.props.toggle}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">
              {T.translate(`${SUFFIX}.Parsers.${parser}.fieldLabel`)}
            </label>
            <input
              type="text"
              className="form-control mousetrap"
              placeholder={T.translate(`${SUFFIX}.Parsers.${parser}.placeholder`)}
              value={this.state.text}
              onChange={this.onTextChange}
              autoFocus
            />
          </div>

          {this.renderOptionalField()}
        </ModalBody>

        <ModalFooter>
          <button className="btn btn-primary" disabled={disabled} onClick={this.apply}>
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

SingleFieldModal.propTypes = {
  toggle: PropTypes.func,
  parser: PropTypes.string,
  onApply: PropTypes.func,
  isTextRequired: PropTypes.bool,
  defaultValue: PropTypes.string,
  hasOptionalField: PropTypes.bool,
};
