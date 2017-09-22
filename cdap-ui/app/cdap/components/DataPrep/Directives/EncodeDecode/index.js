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
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {setPopoverOffset} from 'components/DataPrep/helper';
import {preventPropagation} from 'services/helpers';

require('./EncodeDecode.scss');
const PREFIX = 'features.DataPrep.Directives.Encode';
const ENCODEOPTIONS = [
  {
    label: T.translate(`${PREFIX}.base64`),
    getDirective: (column) => `encode base64 ${column}`
  },
  {
    label: T.translate(`${PREFIX}.base32`),
    getDirective: (column) => `encode base32 ${column}`
  },
  {
    label: T.translate(`${PREFIX}.hex`),
    getDirective: (column) => `encode hex ${column}`
  },
  {
    label: T.translate(`${PREFIX}.url`),
    getDirective: (column) => `url-encode ${column}`
  }
];
export default class EncodeDecode extends Component {
  constructor(props) {
    super(props);
    this.preventPropagation = preventPropagation;
  }
  componentDidUpdate() {
    if (this.props.isOpen && !this.props.isDisabled && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById(`${this.props.directive}-directive`));
  }

  applyDirective({getDirective = () => {}}) {
    let directive = getDirective(this.props.column);
    if (!directive) {
      return;
    }
    execute([directive])
      .subscribe(
        () => {
          this.props.onComplete();
        },
        (err) => {
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
  renderDetail() {
    if (!this.props.isOpen || this.props.isDisabled) {
      return;
    }
    return (
      <div
        className="encode-decode-options second-level-popover"
        onClick={this.preventPropagation}
      >
        {
          this.props.options.map((option, i) => {
            return (
              <div
                className="option"
                key={i}
                onClick={this.applyDirective.bind(this, option)}
              >
                {option.label}
              </div>
            );
          })
        }
      </div>
    );
  }
  render() {
    let id = `${this.props.directive}-directive`;
    return (
      <div>
        <div
          id={id}
          className={classnames('encode-decode-directive clearfix action-item', {
            'active': this.props.isOpen && !this.props.isDisabled,
            'disabled': this.props.isDisabled
          })}
        >
          <span>{this.props.mainMenuLabel}</span>

          <span className="float-xs-right">
            <span className="fa fa-caret-right" />
          </span>

          {this.renderDetail()}
        </div>
        {
          this.props.isDisabled && this.props.disabledTooltip ? (
            <UncontrolledTooltip
              target={id}
              delay={{show: 250, hide: 0}}
            >
              {this.props.disabledTooltip}
            </UncontrolledTooltip>
          ) : null
        }
      </div>
    );
  }
}

EncodeDecode.defaultProps = {
  options: ENCODEOPTIONS,
  directive: 'encode',
  mainMenuLabel: T.translate(`${PREFIX}.title`)
};

EncodeDecode.propTypes = {
  column: PropTypes.string,
  isOpen: PropTypes.bool,
  isDisabled: PropTypes.bool,
  disabledTooltip: PropTypes.string,
  onComplete: PropTypes.func,
  options: PropTypes.arrayOf(PropTypes.object),
  directive: PropTypes.string,
  mainMenuLabel: PropTypes.string
};
