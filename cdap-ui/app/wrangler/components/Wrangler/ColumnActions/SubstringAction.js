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

import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';

export default class SubstringAction extends Component {
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
      this.beginIndex.focus();
    }
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onSave() {
    const beginIndex = this.substringBeginIndex;
    const endIndex = this.substringEndIndex;
    const columnName = this.substringColumnName;

    if (!beginIndex || !columnName) {
      this.setState({error: true});
      return;
    }

    WranglerStore.dispatch({
      type: WranglerActions.subStringColumn,
      payload: {
        activeColumn: this.props.column,
        beginIndex,
        endIndex,
        columnName
      }
    });

    this.setState({isOpen: false});
  }

  renderModal() {
    if (!this.state.isOpen) { return null; }

    const error = (
      <p className="error-text pull-left">
        Please fill out all required fields
      </p>
    );

    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.toggle}
        className="wrangler-actions"
        onClick={e => e.stopPropagation() }
      >
        <ModalHeader>Substring Column: {this.props.column}</ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">
              Begin Index
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="number"
              className="form-control"
              onChange={e => this.substringBeginIndex = e.target.value}
              ref={(ref) => this.beginIndex = ref}
            />
          </div>

          <div>
            <label className="control-label">
              End Index
            </label>
            <input
              type="number"
              className="form-control"
              onChange={e => this.substringEndIndex = e.target.value}
            />
          </div>

          <div>
            <label className="control-label">
              Substring Column Name
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.substringColumnName = e.target.value}
            />
          </div>
        </ModalBody>

        <ModalFooter>
          {this.state.error ? error : null}

          <button
            className="btn btn-success"
            onClick={this.onSave}
          >
            Substring
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    return (
      <span className="column-actions rename-action">
        <span
          className="fa fa-scissors"
          onClick={this.toggle}
        />

        {this.renderModal()}

      </span>
    );
  }
}

SubstringAction.propTypes = {
  column: PropTypes.string
};
