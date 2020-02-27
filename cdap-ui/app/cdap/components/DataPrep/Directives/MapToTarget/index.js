/*
 * Copyright © 2020 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import classnames from 'classnames';

const PREFIX = 'features.DataPrep.Directives.MapToTarget';

export default class MapToTarget extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    isDisabled: PropTypes.bool,
    column: PropTypes.string,
    onComplete: PropTypes.func,
  };

  render() {
    return (
      <div
        id='map-to-target-directive'
        className={classnames('clearfix action-item', {
          active: this.props.isOpen && !this.props.isDisabled,
          disabled: this.props.isDisabled,
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className='float-right'>
          <span className='fa fa-caret-right' />
        </span>
      </div>
    );
  }

}
