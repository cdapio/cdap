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

export default class RenameAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false
    };

    this.toggle = this.toggle.bind(this);
    this.onSave = this.onSave.bind(this);
  }

  componentDidUpdate() {
    if (this.state.isOpen) {
      this.inputName.focus();
    }
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onSave() {
    if (!this.newName) { return; }

    WranglerStore.dispatch({
      type: WranglerActions.renameColumn,
      payload: {
        activeColumn: this.props.column,
        newName: this.newName
      }
    });

    this.setState({isOpen: false});
  }

  handleKeypress(event) {
    if (event.keyCode !== 13) { return; }
    this.onSave();
  }

  renderModal() {
    if (!this.state.isOpen) { return null; }

    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.toggle}
        className="wrangler-actions"
        onClick={e => e.stopPropagation() }
      >
        <ModalHeader>Rename Column: {this.props.column}</ModalHeader>
        <ModalBody>
          <label className="control-label">New column name</label>
          <input
            type="text"
            className="form-control"
            onChange={e => this.newName = e.target.value}
            onKeyDown={this.handleKeypress.bind(this)}
            ref={(ref) => this.inputName = ref}
          />
        </ModalBody>

        <ModalFooter>
          <button
            className="btn btn-success"
            onClick={this.onSave}
          >
            Rename
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    return (
      <span className="column-actions rename-action">
        <span
          className="fa fa-pencil"
          onClick={this.toggle}
        />

        {this.renderModal()}

      </span>
    );
  }
}

RenameAction.propTypes = {
  column: PropTypes.string
};
