/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import SelectWithOptions from 'components/SelectWithOptions';
import range from 'lodash/range';
import T from 'i18n-react';

const PREFIX = 'features.PipelineResources';
const CORE_OPTIONS = range(1, 21);

export default function PipelineResources({virtualCores, onVirtualCoresChange, memoryMB, onMemoryMBChange}) {
  return (
    <div className="resource-holder">
      <div className="resource-group row">
        <span className="col-xs-4 control-label">
          {T.translate(`${PREFIX}.cpu`)}
        </span>
        <SelectWithOptions
          className="small-dropdown form-control"
          options={CORE_OPTIONS}
          value={virtualCores}
          onChange={onVirtualCoresChange}
        />
      </div>
      <div className="resource-group row">
        <span className="col-xs-4 control-label">
          {T.translate(`${PREFIX}.memory`)}
        </span>
        <input
          className="memoryMB-input"
          type="number"
          min={0}
          value={memoryMB}
          onChange={onMemoryMBChange}
        />
        <span className="control-label mb">
          {T.translate(`${PREFIX}.mb`)}
        </span>
      </div>
    </div>
  );
}

PipelineResources.propTypes = {
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func
};
