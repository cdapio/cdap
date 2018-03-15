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
import {connect} from 'react-redux';
import SelectWithOptions from 'components/SelectWithOptions';
import {BATCH_INTERVAL_RANGE, BATCH_INTERVAL_UNITS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.PipelineConfig';

const mapStateToSelectBatchIntervalRangeProps = (state) => {
  return {
    value: state.batchInterval.slice(0, -1),
    options: BATCH_INTERVAL_RANGE
  };
};
const mapDispatchToSelectBatchIntervalRangeProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_BATCH_INTERVAL_RANGE,
        payload: { batchIntervalRange: e.target.value }
      });
      updatePipelineEditStatus();
    }
  };
};

const SelectBatchIntervalRange = connect(
  mapStateToSelectBatchIntervalRangeProps,
  mapDispatchToSelectBatchIntervalRangeProps
)(SelectWithOptions);

const mapStateToSelectBatchIntervalUnitProps = (state) => {
  return {
    value: state.batchInterval.slice(-1),
    options: BATCH_INTERVAL_UNITS
  };
};
const mapDispatchToSelectBatchIntervalUnitProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_BATCH_INTERVAL_UNIT,
        payload: { batchIntervalUnit: e.target.value }
      });
    }
  };
};

const SelectBatchIntervalUnit = connect(
  mapStateToSelectBatchIntervalUnitProps,
  mapDispatchToSelectBatchIntervalUnitProps
)(SelectWithOptions);

const BatchInterval = () => {
  return (
    <div className="label-with-toggle batch-interval form-group row">
      <span className="toggle-label col-xs-4">
        {T.translate(`${PREFIX}.batchInterval`)}
      </span>
      <div className="col-xs-7">
        <SelectBatchIntervalRange className="form-control small-dropdown" />
        <SelectBatchIntervalUnit className="form-control small-dropdown batch-interval-unit" />
      </div>
    </div>
  );
};

export default BatchInterval;
