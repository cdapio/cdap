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
import {Modal, ModalHeader, ModalBody, ModalFooter, Form, FormGroup, Label, Col, Input} from 'reactstrap';
import T from 'i18n-react';

require('./SamplerOptionsModal.scss');

const PREFIX = `features.DataPrep.SamplerDropdownModal`;
const NUMLINES = 10000;
const DEFAULT_FRACTION = 0.35;

export default class SamplerOptionsModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      numLines: props.numLines,
      fraction: props.fraction
    };
    this.applyDirective = this.applyDirective.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
    this.onNumLinesChange = this.onNumLinesChange.bind(this);
    this.onFractionChange = this.onFractionChange.bind(this);
  }
  onNumLinesChange(e) {
    this.setState({
      numLines: e.target.value
    });
  }
  onFractionChange(e) {
    let {value} = e.target;
    let fraction = parseFloat(value);
    if (fraction > 1 || fraction < 0 || typeof fraction !== 'number') {
      return;
    }
    this.setState({
      fraction: fraction
    });
  }
  toggleModal() {
    if (this.props.onClose) {
      this.props.onClose();
    }
  }
  applyDirective() {
    if (this.props.onApply) {
      this.props.onApply({
        numLines: this.state.numLines,
        fraction: this.state.fraction,
        samplerMethod: this.props.samplerMethod
      });
    }
  }
  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.toggleModal}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal sampler-options-modal"
      >
        <ModalHeader>

          <span>
            {T.translate(`${PREFIX}.modalTitle`, {name: this.props.samplerMethod})}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.toggleModal}
          >
            <span className="fa fa-times" />
          </div>

        </ModalHeader>
        <ModalBody>
          <Form>
            <FormGroup className="lines-container" row>
              <Label className="text-xs-right" xs="4"> Show first </Label>
              <Col xs="6">
                <Input
                  type="number"
                  value={this.state.numLines}
                  onChange={this.onNumLinesChange}
                />
                <span>Lines</span>
              </Col>
            </FormGroup>
            <FormGroup className="lines-container" row>
              <Label className="text-xs-right" xs="4"> Probablity </Label>
              <Col xs="6">
                <Input
                  type="number"
                  step="0.01"
                  max="1.00"
                  min="0"
                  value={this.state.fraction}
                  onChange={this.onFractionChange}
                />
              </Col>
            </FormGroup>
          </Form>
        </ModalBody>
        <ModalFooter>
          <button
            className="btn btn-primary"
            onClick={this.applyDirective}
          >
            {T.translate(`${PREFIX}.applyBtnLabel`)}
          </button>
          <button
            className="btn btn-secondary"
            onClick={this.toggleModal}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }
}

SamplerOptionsModal.defaultProps = {
  numLines: NUMLINES,
  fraction: DEFAULT_FRACTION
};

SamplerOptionsModal.propTypes = {
  onClose: PropTypes.func,
  onApply: PropTypes.func,
  numLines: PropTypes.number,
  fraction: PropTypes.number,
  samplerMethod: PropTypes.string
};
