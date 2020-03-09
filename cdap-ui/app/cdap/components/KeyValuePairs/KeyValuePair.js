/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
require('./KeyValuePairs.scss');
import T from 'i18n-react';
import classnames from 'classnames';
import { preventPropagation } from 'services/helpers';

class KeyValuePair extends Component {
  static propTypes = {
    className: PropTypes.string,
    name: PropTypes.string,
    value: PropTypes.string,
    index: PropTypes.number,
    notDeletable: PropTypes.bool,
    provided: PropTypes.bool,
    showReset: PropTypes.bool,
    onChange: PropTypes.func,
    addRow: PropTypes.func,
    removeRow: PropTypes.func,
    onProvided: PropTypes.func,
    getResettedKeyValue: PropTypes.func,
    keyPlaceholder: PropTypes.string,
    valuePlaceholder: PropTypes.string,
    disabled: PropTypes.bool,
    onPaste: PropTypes.func,
  };

  static defaultProps = {
    getResettedKeyValue: () => {},
  };

  keyDown = (e) => {
    if (e.keyCode === 13) {
      this.props.addRow();
    }
  };

  renderKeyField() {
    let keyPlaceholder = '';
    if (!this.props.disabled) {
      keyPlaceholder =
        this.props.keyPlaceholder || T.translate('commons.keyValPairs.keyPlaceholder');
    }

    return (
      <input
        type="text"
        value={this.props.name}
        autoFocus
        onKeyDown={this.keyDown}
        onChange={this.props.onChange.bind(null, 'key')}
        placeholder={keyPlaceholder}
        className={classnames('form-control key-input', { wider: this.props.disabled })}
        disabled={this.props.notDeletable || this.props.disabled}
      />
    );
  }

  renderValueField() {
    if (this.props.provided) {
      return (
        <input
          type="text"
          value=""
          className={classnames('form-control value-input', { wider: this.props.disabled })}
          disabled
        />
      );
    }

    let valuePlaceholder = '';
    if (!this.props.disabled) {
      valuePlaceholder =
        this.props.valuePlaceholder || T.translate('commons.keyValPairs.valuePlaceholder');
    }

    return (
      <input
        type="text"
        value={this.props.value}
        onKeyDown={this.keyDown}
        onChange={this.props.onChange.bind(null, 'value')}
        placeholder={valuePlaceholder}
        className={classnames('form-control value-input', { wider: this.props.disabled })}
        disabled={this.props.disabled}
      />
    );
  }

  renderActionButtons() {
    if (this.props.disabled) {
      return null;
    }

    return (
      <span>
        <button
          type="submit"
          className="btn add-row-btn btn-link"
          onClick={(e) => {
            this.props.addRow();
            preventPropagation(e);
          }}
        >
          <i className="fa fa-plus" />
        </button>
        <button
          type="submit"
          className={classnames('btn remove-row-btn btn-link', {
            invisible: this.props.notDeletable,
          })}
          onClick={(e) => {
            this.props.removeRow();
            preventPropagation(e);
          }}
        >
          <i className="fa fa-trash" />
        </button>
      </span>
    );
  }

  getResettedKeyValue = (index, e) => {
    this.props.getResettedKeyValue(index);
    preventPropagation(e);
  };

  render() {
    return (
      <div className="key-value-pair-preference" data-cy={`key-value-pair-${this.props.index}`}>
        {this.renderKeyField()}
        {this.renderValueField()}
        {this.renderActionButtons()}
      </div>
    );
  }
}

export default KeyValuePair;
