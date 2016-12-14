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

export default class MergeAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      error: false,
      columnList: [],
      mergeWith: ''
    };

    this.toggle = this.toggle.bind(this);
    this.onSave = this.onSave.bind(this);
  }

  componentWillMount() {
    let data = WranglerStore.getState().wrangler.data;
    let columns = Object.keys(data[0]);
    columns.splice(columns.indexOf(this.props.column), 1);
    this.setState({
      columnList: columns,
      mergeWith: columns[0]
    });
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onSave() {
    const mergeWith = this.state.mergeWith;
    const joinBy = this.joinBy;
    const mergedColumnName = this.mergedColumnName;

    if (!mergeWith || !joinBy || !mergedColumnName) {
      this.setState({error: true});
      return;
    }

    WranglerStore.dispatch({
      type: WranglerActions.mergeColumn,
      payload: {
        activeColumn: this.props.column,
        joinBy,
        mergedColumnName,
        mergeWith
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
        zIndex="1070"
      >
        <ModalHeader>Merge Column: {this.props.column}</ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">
              Merge With
              <span className="fa fa-asterisk error-text"></span>
            </label>

            <select
              className="form-control"
              value={this.state.mergeWith}
              onChange={e => this.setState({mergeWith: e.target.value})}
            >
              {
                this.state.columnList.map((header) => {
                  return (
                    <option
                      value={header}
                      key={header}
                    >
                      {header}
                    </option>
                  );
                })
              }
            </select>
          </div>

          <div>
            <label className="control-label">
              Join By
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.joinBy = e.target.value}
            />
          </div>

          <div>
            <label className="control-label">
              Merged Column Name
              <span className="fa fa-asterisk error-text"></span>
            </label>
            <input
              type="text"
              className="form-control"
              onChange={e => this.mergedColumnName = e.target.value}
            />
          </div>
        </ModalBody>

        <ModalFooter>
          {this.state.error ? error : null}

          <button
            className="btn btn-success"
            onClick={this.onSave}
          >
            Merge
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    return (
      <span className="column-actions rename-action">
        <span
          className="fa fa-compress"
          onClick={this.toggle}
        />

        {this.renderModal()}

      </span>
    );
  }
}

MergeAction.propTypes = {
  column: PropTypes.string
};
