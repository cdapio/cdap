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
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {preventPropagation} from 'services/helpers';
import Mousetrap from 'mousetrap';

require('./CustomTransform.scss');

const PREFIX = 'features.DataPrep.Directives.CustomTransform';

export default class CustomTransform extends Component {
  state = {
    input: ''
  };

  componentDidMount() {
    Mousetrap.bind('enter', this.applyDirective);
  }

  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  handleInputChange = (e) => {
    this.setState({input: e.target.value});
  }

  applyDirective = () => {
    if (this.state.input.length === 0) { return; }

    let directive = `set-column ${this.props.column} ${this.state.input}`;

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
        className="second-level-popover custom-transform-popover"
        onClick={preventPropagation}
      >
       <h5>{T.translate(`${PREFIX}.description`, {column: this.props.column})}</h5>

        <textarea
          rows="6"
          className="form-control mousetrap"
          value={this.state.input}
          onChange={this.handleInputChange}
          placeholder={T.translate(`${PREFIX}.placeholder`, {column: this.props.column})}
          autoFocus
        />

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={this.state.input.length === 0}
          >
            {T.translate('features.DataPrep.Directives.apply')}
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
        id="custom-transform-directive"
        className={classnames('clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>
          {T.translate(`${PREFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <IconSVG name="icon-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

CustomTransform.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
