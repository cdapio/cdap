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
import SelectWithOptions from 'components/SelectWithOptions';
import {MAX_CONCURRENT_RUNS_OPTIONS, ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';
import {connect} from 'react-redux';
import T from 'i18n-react';

const PREFIX = 'features.PipelineScheduler';

const mapStateToMaxConcurrentRunsProps = (state) => {
  return {
    options: MAX_CONCURRENT_RUNS_OPTIONS,
    value: state.maxConcurrentRuns
  };
};
const mapDispatchToMaxConcurrentRunsProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_MAX_CONCURRENT_RUNS,
        payload: {
          maxConcurrentRuns: e.target.value
        }
      });
    }
  };
};

const SelectMaxConcurrentRuns = connect(
  mapStateToMaxConcurrentRunsProps,
  mapDispatchToMaxConcurrentRunsProps
)(SelectWithOptions);

export default function MaxConcurrentRuns() {
  return (
    <div className="form-group row max-concurrent-runs">
      <label className="col-xs-3 control-label">
        {T.translate(`${PREFIX}.maxConcurrentRuns`)}
      </label>
      <div className="col-xs-4 schedule-values-container">
        <span className="schedule-values">
          <SelectMaxConcurrentRuns className="form-control small-dropdown" />
        </span>
      </div>
    </div>
  );
}

