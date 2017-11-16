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
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';
import ee from 'event-emitter';
import T from 'i18n-react';
import {preventPropagation} from 'services/helpers';

const PREFIX = 'features.DataPrep.Directives.ChangeDataType';

require('./ChangeDataType.scss');

const DATATYPE_OPTIONS = [
  'string',
  'boolean',
  'integer',
  'long',
  'short',
  'float',
  'double',
  'bytes'
];

export default class ChangeDataTypeDirective extends Component {
  constructor(props) {
    super(props);

    this.columnType = DataPrepStore.getState().dataprep.types[this.props.column];

    this.state = {
      selectedChangeDataType: null
    };

    window.addEventListener('resize', this.offsetCalcDebounce);

    this.eventEmitter = ee(ee);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('change-data-type-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
   }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }

  applyDirective(option) {
    if (this.columnType.toUpperCase() === option.toUpperCase()) {
      return;
    }

    let directive = `set-type :${this.props.column} ${option}`;
    execute([directive])
      .subscribe(
        () => {
          this.eventEmitter.emit('DATAPREP_DATA_TYPE_CHANGED', this.props.column);
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
        className="change-data-type-options second-level-popover"
        onClick={preventPropagation}
      >
        {
          this.props.options.map((option, i) => {
            return (
              <div
                className={classnames('option', {
                             'disabled': this.columnType.toUpperCase() === option.toUpperCase()
                           })}
                key={i}
                onClick={this.applyDirective.bind(this, option)}
              >
                {T.translate(`${PREFIX}.Options.${option}`)}
              </div>
            );
          })
        }
      </div>
    );
  }

  render() {
    return (
      <div
        id="change-data-type-directive"
        className={classnames('change-data-type-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>
          {T.translate(`${PREFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

ChangeDataTypeDirective.defaultProps = {
  options: DATATYPE_OPTIONS
};

ChangeDataTypeDirective.propTypes = {
  column: PropTypes.string,
  options: PropTypes.arrayOf(PropTypes.object),
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  isDisabled: PropTypes.bool,
  close: PropTypes.func
};
