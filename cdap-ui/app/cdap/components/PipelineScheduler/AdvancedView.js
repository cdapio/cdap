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
import {connect} from 'react-redux';
import {setStateFromCron} from 'components/PipelineScheduler/Store/ActionCreator';
import {ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';

const mapStateToCronInputProps = (state, ownProps) => {
  return {
    value: state.cron.split(" ")[ownProps.index],
    label: ownProps.label,
    colWidth: ownProps.colWidth
  };
};

const mapDispatchToCronInputProps = (dispatch, ownProps) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.UPDATE_CRON,
        payload: {
          index: ownProps.index,
          value: e.target.value
        }
      });
      setStateFromCron();
    }
  };
};

const CronInput = ({value, label, onChange, colWidth = 2}) => {
  return (
    <div className={`form-group col-xs-${colWidth} schedule-advanced-input`}>
      <label>{label}</label>
      <input
        type="text"
        value={value}
        onChange={onChange}
        className="form-control"
      />
    </div>
  );
};

CronInput.propTypes = {
  value: PropTypes.string,
  label: PropTypes.string,
  colWidth: PropTypes.number,
  onChange: PropTypes.func
};

const ConnectedCronInput = connect(
  mapStateToCronInputProps,
  mapDispatchToCronInputProps
)(CronInput);

export default function AdvancedView() {
  return (
    <div className="schedule-type-content">
      <div className="schedule-advanced-header">
        Schedule this pipeline by using Cron syntax
      </div>
      <div className="schedule-advanced-values">
        <ConnectedCronInput
          label={"Min"}
          index={0}
        />
        <ConnectedCronInput
          label={"Hour"}
          index={1}
        />
        <ConnectedCronInput
          label={"Day"}
          index={2}
        />
        <ConnectedCronInput
          label={"Month"}
          index={3}
        />
        <ConnectedCronInput
          label={"Days of the Week"}
          index={4}
          colWidth={3}
        />
      </div>
    </div>
  );
}
