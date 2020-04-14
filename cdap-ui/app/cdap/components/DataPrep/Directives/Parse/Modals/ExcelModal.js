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
import { Modal, ModalHeader, ModalBody, ModalFooter, FormGroup, Label, Input } from 'reactstrap';
import isNil from 'lodash/isNil';
import MouseTrap from 'mousetrap';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.DataPrep.Directives.Parse.Parsers.EXCEL';

export default class ExcelModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      sheetSource: 'sheetnumber',
      sheetName: null,
      sheetNumber: 0,
      firstRowHeader: false,
    };
    this.onSheetNumberChange = this.onSheetNumberChange.bind(this);
    this.onSheetNameChange = this.onSheetNameChange.bind(this);
    this.onSheetSourceChange = this.onSheetSourceChange.bind(this);
    this.toggleSetFirstRow = this.toggleSetFirstRow.bind(this);
    this.isApplyDisabled = this.isApplyDisabled.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }

  componentDidMount() {
    MouseTrap.bind('enter', this.applyDirective);
  }

  componentWillUnmount() {
    MouseTrap.unbind('enter');
  }

  isApplyDisabled() {
    if (this.state.sheetSource === 'sheetnumber') {
      return typeof this.state.sheetNumber !== 'number' || isNaN(this.state.sheetNumber);
    }
    if (this.state.sheetSource === 'sheetname') {
      return isNil(this.state.sheetName) || this.state.sheetName.length === 0;
    }
    return false;
  }

  onSheetNumberChange(e) {
    let value = parseInt(e.target.value, 10);
    if (value < 0) {
      return;
    }
    this.setState({
      sheetNumber: parseInt(value, 10),
    });
  }

  onSheetNameChange(e) {
    this.setState({
      sheetName: e.target.value,
    });
  }

  onSheetSourceChange(e) {
    this.setState({
      sheetSource: e.target.value,
    });
  }

  toggleSetFirstRow() {
    this.setState({ firstRowHeader: !this.state.firstRowHeader });
  }

  applyDirective() {
    if (this.isApplyDisabled()) {
      return;
    }
    if (this.state.sheetSource === 'sheetnumber') {
      this.props.onApply(`'${this.state.sheetNumber.toString()}' ${this.state.firstRowHeader}`);
      this.props.toggle();
      return;
    }
    if (this.state.sheetSource === 'sheetname') {
      this.props.onApply(`'${this.state.sheetName}' ${this.state.firstRowHeader}`);
      this.props.toggle();
      return;
    }
  }

  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal parse-as-excel cdap-modal"
        autoFocus={false}
      >
        <ModalHeader>
          <span>
            {T.translate(`features.DataPrep.Directives.Parse.modalTitle`, { parser: 'Excel' })}
          </span>

          <div className="close-section float-right" onClick={this.props.toggle}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <p>{T.translate(`${PREFIX}.modal.description`)}</p>
          <FormGroup check>
            <Label check>
              <Input
                type="radio"
                value="sheetnumber"
                min="0"
                onChange={this.onSheetSourceChange}
                checked={this.state.sheetSource === 'sheetnumber'}
              />{' '}
              {T.translate(`${PREFIX}.modal.sheetNumberLabel`)}
            </Label>
            {this.state.sheetSource === 'sheetnumber' ? (
              <input
                type="number"
                className="form-control mousetrap"
                value={this.state.sheetNumber}
                onChange={this.onSheetNumberChange}
                autoFocus
              />
            ) : null}
          </FormGroup>
          <FormGroup check>
            <Label check>
              <Input
                type="radio"
                value="sheetname"
                onChange={this.onSheetSourceChange}
                checked={this.state.sheetSource === 'sheetname'}
              />{' '}
              {T.translate(`${PREFIX}.modal.sheetNameLabel`)}
            </Label>
            {this.state.sheetSource === 'sheetname' ? (
              <input
                type="text"
                className="form-control mousetrap"
                value={this.state.sheetName}
                onChange={this.onSheetNameChange}
                placeholder={T.translate(`${PREFIX}.modal.sheetNameInputPlaceholder`)}
                autoFocus
              />
            ) : null}
          </FormGroup>
          <div className="optional-config">
            <span onClick={this.toggleSetFirstRow}>
              <span
                className={classnames('fa', {
                  'fa-square-o': !this.state.firstRowHeader,
                  'fa-check-square': this.state.firstRowHeader,
                })}
              />
              <span>{T.translate(`${PREFIX}.modal.firstRowHeader`)}</span>
            </span>
          </div>
        </ModalBody>
        <ModalFooter>
          <button
            className="btn btn-primary"
            disabled={this.isApplyDisabled() ? 'disabled' : null}
            onClick={this.applyDirective}
          >
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

ExcelModal.propTypes = {
  toggle: PropTypes.func,
  onApply: PropTypes.func,
};
