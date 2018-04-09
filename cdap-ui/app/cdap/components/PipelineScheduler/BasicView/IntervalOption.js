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
import {INTERVAL_OPTIONS, ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';
import {updateCron} from 'components/PipelineScheduler/Store/ActionCreator';
import {connect} from 'react-redux';
import T from 'i18n-react';

const PREFIX = 'features.PipelineScheduler.intervalOptions';

const mapStateToIntervalOptionProps = (state) => {
  return {
    options: Object.values(INTERVAL_OPTIONS),
    value: state.intervalOption
  };
};
const mapDispatchToIntervalOptionProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.CRON_RESET
      });
      dispatch({
        type: PipelineSchedulerActions.SET_INTERVAL_OPTION,
        payload: {
          intervalOption: e.target.value
        }
      });
      updateCron();
    }
  };
};

const SelectIntervalOption = connect(
  mapStateToIntervalOptionProps,
  mapDispatchToIntervalOptionProps
)(SelectWithOptions);

export default function IntervalOption() {
  return (
    <div className="form-group row">
      <label className="col-xs-3 control-label">
        {T.translate(`${PREFIX}.heading`)}
      </label>
      <div className="col-xs-4 schedule-values-container">
        <SelectIntervalOption className="form-control" />
      </div>
    </div>
  );
}
