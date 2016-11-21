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
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';

import WranglerActions from 'components/Wrangler/Store/WranglerActions';
import WranglerStore from 'components/Wrangler/Store/WranglerStore';

export default class SplitAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      error: false
    };

    this.toggle = this.toggle.bind(this);
    this.onSave = this.onSave.bind(this);
  }

  componentDidUpdate() {
    if (this.state.isOpen) {
      this.delimiterInput.focus();
    }
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onSave() {
    const delimiter = this.delimiter;
    const firstSplit = this.firstSplit;
    const secondSplit = this.secondSplit;

    if (!delimiter || !firstSplit || !secondSplit ) {
      this.setState({error: true});
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
        Please fill out all fields
      </p>
    );

    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.toggle}
        className="wrangler-actions"
      >
        <ModalHeader>Split Column: {this.props.column}</ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">Split by first occurrence of</label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.delimiter = e.target.value}
              ref={(ref) => this.delimiterInput = ref}
            />
          </div>

          <div>
            <label className="control-label">First Split Column Name</label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.firstSplit = e.target.value}
            />
          </div>

          <div>
            <label className="control-label">Second Split Column Name</label>
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
            className="btn btn-success"
            onClick={this.onSave}
          >
            Split
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    return (
      <span className="column-actions split-action">
        <span
          className="fa fa-columns"
          onClick={this.toggle}
        />

        {this.renderModal()}
      </span>
    );
  }
}

SplitAction.propTypes = {
  column: PropTypes.string
};
