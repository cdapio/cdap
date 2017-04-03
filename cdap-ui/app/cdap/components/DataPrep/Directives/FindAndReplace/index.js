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

import React, { Component, PropTypes } from 'react';
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import T from 'i18n-react';

const PREFIX = `features.DataPrep.Directives.FindAndReplace`;

export default class FindAndReplaceDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      findInput: '',
      replaceInput: ''
    };

    this.handleFindInputChange = this.handleFindInputChange.bind(this);
    this.handleReplaceInputChange = this.handleReplaceInputChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.state.findInput.length === 0 && this.findInputBox) {
      this.findInputBox.focus();
    }
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  handleFindInputChange(e) {
    this.setState({findInput: e.target.value});
  }

  handleReplaceInputChange(e) {
    this.setState({replaceInput: e.target.value});
  }

  applyDirective() {
    if (this.state.findInput.length === 0) { return; }
    let column = this.props.column;
    let findInput = this.state.findInput;
    let replaceInput = this.state.replaceInput;

    let directive = `find-and-replace ${column} s/${findInput}/${replaceInput}/g`;

    execute([directive])
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
      }, (err) => {
        console.log('error', err);

        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      });
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="fill-null-or-empty-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        <h5>{T.translate(`${PREFIX}.find`)}</h5>

        <div className="input">
          <input
            type="text"
            className="form-control"
            value={this.state.findInput}
            onChange={this.handleFindInputChange}
            placeholder={T.translate(`${PREFIX}.findPlaceholder`)}
            ref={ref => this.findInputBox = ref}
          />
        </div>

        <br />

        <h5>{T.translate(`${PREFIX}.replaceWith`)}</h5>

        <div className="input">
          <input
            type="text"
            className="form-control"
            value={this.state.replaceInput}
            onChange={this.handleReplaceInputChange}
            placeholder={T.translate(`${PREFIX}.replacePlaceholder`)}
          />
        </div>

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={this.state.findInput.length === 0}
          >
            {T.translate(`${PREFIX}.buttonLabel`)}
          </button>

          <button
            className="btn btn-link float-xs-right"
            onClick={this.props.close}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </div>

      </div>
    );
  }

  render() {
    return (
      <div
        className={classnames('fill-null-or-empty-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

FindAndReplaceDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
