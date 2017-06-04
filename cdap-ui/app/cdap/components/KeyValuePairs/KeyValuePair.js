/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
require('./KeyValuePairs.scss');
import T from 'i18n-react';
import classnames from 'classnames';

class KeyValuePair extends Component {
  constructor(props) {
    super(props);

    this.keyDown = this.keyDown.bind(this);
  }

  keyDown(e) {
    if (e.keyCode === 13) {
      this.props.addRow();
    }
  }

  renderValueField() {
    if (this.props.provided) {
      return (
        <input
          type="text"
          value=""
          className="form-control value-input"
          disabled
        />
      );
    }
    return (
      <input
        type="text"
        value={this.props.value}
        onKeyDown={this.keyDown}
        onChange={this.props.onChange.bind(null, 'value')}
        placeholder={T.translate('commons.keyValPairs.valuePlaceholder')}
        className="form-control value-input"
      />
    );
  }

  renderNotDeletableElements() {
    if (!this.props.notDeletable) { return null; }

    return (
      <span>
        <input
          type="checkbox"
          checked={this.props.provided}
          onChange={this.props.onProvided}
          className="form-control provided-input"
        />
        <span
          className={classnames("reset-action", {"hidden": !this.props.showReset})}
          onClick={this.props.getResettedKeyValue.bind(this, this.props.index)}
        >
          {T.translate('commons.keyValPairs.reset')}
        </span>
      </span>
    );
  }

  render() {
    return (
      <div className="key-value-pair-preference">
        <input
          type="text"
          value={this.props.name}
          autoFocus
          onKeyDown={this.keyDown}
          onChange={this.props.onChange.bind(null, 'key')}
          placeholder={T.translate('commons.keyValPairs.keyPlaceholder')}
          className="form-control key-input"
          disabled={this.props.notDeletable}
        />
        {this.renderValueField()}
        <button
          type="submit"
          className="btn add-row-btn btn-link"
          onClick={this.props.addRow}
        >
          <i className="fa fa-plus" />
        </button>
        <button
          type="submit"
          className={classnames("btn remove-row-btn btn-link", {"invisible": this.props.notDeletable})}
          onClick={this.props.removeRow}
        >
          <i className="fa fa-trash" />
        </button>
        {this.renderNotDeletableElements()}
      </div>
    );
  }
}

KeyValuePair.propTypes = {
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
  getResettedKeyValue: PropTypes.func
};

export default KeyValuePair;
