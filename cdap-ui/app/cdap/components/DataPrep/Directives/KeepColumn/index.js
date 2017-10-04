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
const PREFIX = 'features.DataPrep.Directives.Keep';

export default class KeepColumnDirective extends Component {
  constructor(props) {
    super(props);
    this.applyDirective = this.applyDirective.bind(this);
  }

  applyDirective() {
    let column = this.props.column.toString();
    let directive = `keep ${column}`;

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
    let title = T.translate(`${PREFIX}.title.singular`);
    if (Array.isArray(this.props.column) && this.props.column.length >= 2) {
      title = T.translate(`${PREFIX}.title.plural`);
    }

    return (
      <div
        className="keep-column-directive clearfix action-item"
        onClick={this.applyDirective}
      >
        <span>
          {title}
        </span>
      </div>
    );
  }
}

KeepColumnDirective.propTypes = {
  column: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.array
  ]),
  onComplete: PropTypes.func
};
