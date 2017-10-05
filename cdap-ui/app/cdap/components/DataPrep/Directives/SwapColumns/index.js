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
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

export default class SwapColumnsDirective extends Component {
  constructor(props) {
    super(props);
    this.applyDirective = this.applyDirective.bind(this);
  }

  applyDirective() {
    let columns = this.props.column;
    let directive = `swap ${columns[0]} ${columns[1]}`;

    execute([directive])
      .subscribe(() => {
        this.props.onComplete();
      }, (err) => {
        console.log('Error', err);

        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      });
  }

  render() {
    return (
      <div
        className="swap-column-directive clearfix action-item"
        onClick={!this.props.isDisabled && this.applyDirective}
      >
        <span>
          {T.translate('features.DataPrep.Directives.Swap.title')}
        </span>
      </div>
    );
  }
}

SwapColumnsDirective.propTypes = {
  column: PropTypes.oneOfType([PropTypes.array, PropTypes.string]),
  onComplete: PropTypes.func,
  isDisabled: PropTypes.bool
};
