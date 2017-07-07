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
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import DataPrepStore from 'components/DataPrep/store';
import T from 'i18n-react';
require('./MaskData.scss');

const PREFIX = 'features.DataPrep.Directives.MaskData';

export default class MaskData extends Component {
  constructor(props) {
    super(props);
    this.maskLast4Digits = this.maskLast4Digits.bind(this);
    this.maskLast2Digits = this.maskLast2Digits.bind(this);
    this.maskCustomSelection = this.maskCustomSelection.bind(this);
    this.maskByShuffling = this.maskByShuffling.bind(this);
    this.isDirectiveEnabled = this.isDirectiveEnabled.bind(this);
  }
  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('mask-fields-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset && this.isDirectiveEnabled()) {
      this.calculateOffset();
    }
  }
  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }
  maskCustomSelection() {
    let {highlightColumns} = DataPrepStore.getState().dataprep;
    DataPrepStore.dispatch({
      type: DataPrepActions.setHighlightColumns,
      payload: {
        highlightColumns: {
          columns: highlightColumns.columns.concat([this.props.column]),
          directive: 'MASK'
        }
      }
    });
    this.props.onComplete();
  }
  maskByShuffling() {
    this.applyDirective(`mask-shuffle ${this.props.column}`);
  }
  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {
          this.props.onComplete();
        }, (err) => {
          console.log('error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }
  isDirectiveEnabled() {
    let {types} = DataPrepStore.getState().dataprep;
    return types[this.props.column] === 'string';
  }
  maskLastNDigits(N) {
    let {data} = DataPrepStore.getState().dataprep;
    let length = data[0][this.props.column].length;
    let maskPattern = Array.apply(null, {length: length - N}).map(() => 'x').join('');
    let allowPattern = Array.apply(null, {length: N}).map(() => '#').join('');

    let pattern = maskPattern + allowPattern;
    return pattern;
  }
  maskLast4Digits() {
    let pattern = this.maskLastNDigits(4);
    this.applyDirective(`mask-number ${this.props.column} ${pattern}`);
  }
  maskLast2Digits() {
    let pattern = this.maskLastNDigits(2);
    this.applyDirective(`mask-number ${this.props.column} ${pattern}`);
  }
  renderSubMenu() {
    if (!this.props.isOpen || !this.isDirectiveEnabled()) { return null; }

    return (
      <div
        className="mask-fields second-level-popover"
        onClick={this.preventPropagation}
      >
        <div className="mask-field-options">
          <div
            onClick={this.maskLast4Digits}
            className="option"
          >
            {T.translate(`${PREFIX}.option1`)}
          </div>
        </div>
        <div className="mask-field-options">
          <div
            onClick={this.maskLast2Digits}
            className="option"
          >
            {T.translate(`${PREFIX}.option2`)}
          </div>
        </div>
        <div className="mask-field-options">
          <div
            onClick={this.maskCustomSelection}
            className="option"
          >
            {T.translate(`${PREFIX}.option3`)}
          </div>
        </div>
        <div className="column-action-divider">
          <hr />
        </div>
        <div className="mask-field-options">
          <div
            onClick={this.maskByShuffling}
            className="option"
          >
            {T.translate(`${PREFIX}.option4`)}
          </div>
        </div>
      </div>
    );
  }
  render() {
    return (
      <div
        id="mask-fields-directive"
        className={classnames('clearfix action-item', {
          'active': this.props.isOpen && this.isDirectiveEnabled(),
          'disabled': !this.isDirectiveEnabled()
        })}
      >
        <span className="option">
          {T.translate(`${PREFIX}.menuLabel`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>
        {this.renderSubMenu()}
      </div>
    );
  }
}
MaskData.propTypes = {
  isOpen: PropTypes.bool,
  onComplete: PropTypes.func,
  column: PropTypes.string
};
