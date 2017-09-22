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

require('./DataQuality.scss');

export default function DataQuality({ columnInfo }) {
  if (!columnInfo || !columnInfo.general) { return null; }

  let nonNull = columnInfo.general['non-null'] || 0,
      empty = columnInfo.general['empty'] || 0;

  let filled = Math.round(nonNull - empty);

  return (
    <div className="quality-bar">
      <span
        className="filled"
        style={{width: `${filled}%`}}
      />
      <span
        className="empty"
        style={{width: `${100 - filled}%`}}
      />
    </div>
  );
}

DataQuality.propTypes = {
  columnInfo: PropTypes.shape({
    general: PropTypes.object,
    types: PropTypes.object,
    isValid: PropTypes.bool
  })
};
