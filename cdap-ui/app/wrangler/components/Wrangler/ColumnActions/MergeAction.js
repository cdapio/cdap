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

export default class MergeAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      error: false,
      columnList: [],
      mergeWith: '',
      tooltipOpen: false
    };

    this.toggle = this.toggle.bind(this);
    this.onSave = this.onSave.bind(this);
    this.tooltipToggle = this.tooltipToggle.bind(this);
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

  tooltipToggle() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  onSave() {
    const mergeWith = this.state.mergeWith;
    const joinBy = this.joinBy;
    const mergedColumnName = this.mergedColumnName;

    if (!mergeWith || !joinBy || !mergedColumnName) {
      this.setState({error: 'MISSING_REQUIRED_FIELDS'});
      return;
    }

    let error = validateColumnName(mergedColumnName);
    if (error) {
      this.setState({error});
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
      <p className="error-text float-xs-left">
        {T.translate(`features.Wrangler.Errors.${this.state.error}`)}
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
        <ModalHeader>
          <span>
            {T.translate('features.Wrangler.ColumnActions.Merge.header', {
              columnName: this.props.column
            })}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div>
            <label className="control-label">
              {T.translate('features.Wrangler.ColumnActions.Merge.mergeWith')}
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
              {T.translate('features.Wrangler.ColumnActions.Merge.joinBy')}
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
              {T.translate('features.Wrangler.ColumnActions.Merge.mergedColumn')}
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
            className="btn btn-wrangler"
            onClick={this.onSave}
          >
            {T.translate('features.Wrangler.ColumnActions.Merge.label')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }

  render() {
    const id = 'column-action-merge';

    return (
      <span className="column-actions rename-action">
        <span
          id={id}
          className="fa icon-merge"
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
          {T.translate('features.Wrangler.ColumnActions.Merge.label')}
        </Tooltip>

        {this.renderModal()}

      </span>
    );
  }
}

MergeAction.propTypes = {
  column: PropTypes.string
};
