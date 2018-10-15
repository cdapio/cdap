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
import T from 'i18n-react';
import { preventPropagation } from 'services/helpers';
import classnames from 'classnames';

export default class DSVRow extends Component {
  constructor(props) {
    super(props);

    this.keyDown = this.keyDown.bind(this);
  }

  keyDown(e) {
    if (e.keyCode === 13) {
      preventPropagation(e);
      this.props.addRow();
    }
  }

  renderActionButtons() {
    if (this.props.disabled) {
      return null;
    }

    return (
      <div className="action-buttons-container text-right">
        <button className="btn add-row-btn btn-link" onClick={this.props.addRow}>
          <i className="fa fa-plus" />
        </button>
        <button className="btn remove-row-btn btn-link" onClick={this.props.removeRow}>
          <i className="fa fa-trash text-danger" />
        </button>
      </div>
    );
  }

  render() {
    let placeholder = this.props.placeholder || T.translate('commons.DSVEditor.placeholder');

    return (
      <div className="dsv-row-container">
        <div className={classnames({ disabled: this.props.disabled }, 'dsv-input-container')}>
          <input
            type="text"
            value={this.props.property}
            autoFocus
            onKeyDown={this.keyDown}
            onChange={this.props.onChange.bind(null, 'property')}
            className="form-control"
            placeholder={placeholder}
          />
        </div>
        {this.renderActionButtons()}
      </div>
    );
  }
}

DSVRow.propTypes = {
  className: PropTypes.string,
  property: PropTypes.string,
  index: PropTypes.number,
  onChange: PropTypes.func,
  addRow: PropTypes.func,
  removeRow: PropTypes.func,
  placeholder: PropTypes.string,
  disabled: PropTypes.bool,
};
