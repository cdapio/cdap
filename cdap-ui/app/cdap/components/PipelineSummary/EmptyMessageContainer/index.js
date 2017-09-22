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

import React from 'react';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';

const GRAPHPREFIX = `features.PipelineSummary.graphs`;

require('./EmptyMessageContainer.scss');

export default function EmptyMessageContainer({xDomainType, label, loading, message}) {
  if (loading) {
    return (
      <div className="empty-runs-container">
        <LoadingSVG />
      </div>
    );
  }
  return (
      <div className="empty-runs-container">
        <h1>
          {
            message ?
              message
            :
              T.translate(`${GRAPHPREFIX}.emptyMessage`, {
                filter: xDomainType === 'time' ? `in ${label}` : ''
              })
          }
        </h1>
      </div>
    );
}
EmptyMessageContainer.propTypes = {
  xDomainType: PropTypes.string,
  label: PropTypes.string,
  message: PropTypes.string,
  loading: PropTypes.bool
};
