/*
 * Copyright © 2016 Cask Data, Inc.
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
import { Modal, ModalHeader, ModalBody, ModalFooter, Tooltip } from 'reactstrap';

import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import validateColumnName from 'wrangler/components/Wrangler/column-validation';
import T from 'i18n-react';

export default class SplitAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      error: false,
      tooltipOpen: false
    };

    this.toggle = this.toggle.bind(this);
    this.onSave = this.onSave.bind(this);
    this.tooltipToggle = this.tooltipToggle.bind(this);
  }

  componentDidUpdate() {
    if (this.state.isOpen) {
      this.delimiterInput.focus();
    }
  }

  tooltipToggle() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onSave() {
    const delimiter = this.delimiter;
    const firstSplit = this.firstSplit;
    const secondSplit = this.secondSplit;

    if (!delimiter || !firstSplit || !secondSplit ) {
      this.setState({error: 'MISSING_REQUIRED_FIELDS'});
      return;
    }
    let error = validateColumnName(firstSplit) || validateColumnName(secondSplit);
    if (error) {
      this.setState({error});
      return;
    }

    WranglerStore.dispatch({
      type: WranglerActions.splitColumn,
      payload: {
        activeColumn: this.props.column,
        firstSplit,
        secondSplit,
        delimiter
      }
    });

    this.setState({isOpen: false});
  }

  renderModal() {
    if (!this.state.isOpen) { return null; }

    const error = (
      <p className="error-text pull-left">
        {T.translate(`features.Wrangler.Errors.${this.state.error}`)}
      </p>
    );

    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.toggle}
        className="wrangler-actions"
        zIndex="1070"
      >
        <ModalHeader>
          <span>Split Column: {this.props.column}</span>

          <div
            className="close-section pull-right"
            onClick={this.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">
              Split by first occurrence of
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.delimiter = e.target.value}
              ref={(ref) => this.delimiterInput = ref}
            />
          </div>

          <div>
            <label className="control-label">
              First Split Column Name
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.firstSplit = e.target.value}
            />
          </div>

          <div>
            <label className="control-label">
              Second Split Column Name
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.secondSplit = e.target.value}
            />
          </div>
        </ModalBody>

        <ModalFooter>
          {this.state.error ? error : null}
          <button
            className="btn btn-wrangler"
            onClick={this.onSave}
          >
            Split
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    const id = 'column-action-split';

    return (
      <span className="column-actions split-action">
        <span
          id={id}
          className="fa icon-split"
          onClick={this.toggle}
        />

        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          toggle={this.tooltipToggle}
          target={id}
          className="wrangler-tooltip"
          delay={0}
        >
          Split
        </Tooltip>

        {this.renderModal()}
      </span>
    );
  }
}

SplitAction.propTypes = {
  column: PropTypes.string
};
