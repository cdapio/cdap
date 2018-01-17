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
import classnames from 'classnames';
import SelectWithOptions from 'components/SelectWithOptions';
import {INTERVAL_OPTIONS, MINUTE_OPTIONS, HOUR_OPTIONS_CLOCK, AM_PM_OPTIONS, ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';
import {updateCron} from 'components/PipelineScheduler/Store/ActionCreator';
import {connect} from 'react-redux';

const mapStateToStartingAtMinuteProps = (state) => {
  return {
    options: MINUTE_OPTIONS,
    value: state.startingAtMinute
  };
};
const mapDispatchToStartingAtMinuteProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_STARTING_AT_MINUTE,
        payload: {
          startingAtMinute: e.target.value
        }
      });
      updateCron();
    }
  };
};

const mapStateToStartingAtHourProps = (state) => {
  return {
    options: HOUR_OPTIONS_CLOCK,
    value: state.startingAtHour
  };
};
const mapDispatchToStartingAtHourProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_STARTING_AT_HOUR,
        payload: {
          startingAtHour: e.target.value
        }
      });
      updateCron();
    }
  };
};

const mapStateToStartingAtAMPMProps = (state) => {
  return {
    options: AM_PM_OPTIONS,
    value: state.startingAtAMPM
  };
};
const mapDispatchToStartingAtAMPMProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_STARTING_AT_AM_PM,
        payload: {
          startingAtAMPM: e.target.value
        }
      });
      updateCron();
    }
  };
};

const SelectStartingAtMinute = connect(
  mapStateToStartingAtMinuteProps,
  mapDispatchToStartingAtMinuteProps
)(SelectWithOptions);
const SelectStartingAtHour = connect(
  mapStateToStartingAtHourProps,
  mapDispatchToStartingAtHourProps
)(SelectWithOptions);
const SelectStartingAtAMPM = connect(
  mapStateToStartingAtAMPMProps,
  mapDispatchToStartingAtAMPMProps
)(SelectWithOptions);

const mapStateToStartingAtComponentProps = (state) => {
  return {
    intervalOption: state.intervalOption
  };
};

const StartingAtComponent = ({intervalOption}) => {
  let SelectComponent;

  switch (intervalOption) {
    case INTERVAL_OPTIONS.HOURLY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectStartingAtMinute className="form-control small-dropdown" />
          <span>past the hour</span>
        </span>
      );
      break;
    case INTERVAL_OPTIONS.DAILY:
    case INTERVAL_OPTIONS.WEEKLY:
    case INTERVAL_OPTIONS.MONTHLY:
    case INTERVAL_OPTIONS.YEARLY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectStartingAtHour className="form-control small-dropdown" />
          <span className="separator"> : </span>
          <SelectStartingAtMinute className="form-control small-dropdown" />
          <span>&nbsp;</span>
          <SelectStartingAtAMPM className="form-control small-dropdown" />
        </span>
      );
      break;
  }

  let shouldHideComponent = [INTERVAL_OPTIONS['5MIN'], INTERVAL_OPTIONS['10MIN'], INTERVAL_OPTIONS['30MIN']].indexOf(intervalOption) !== -1;

  return (
    <div className={classnames('form-group row', {'invisible': shouldHideComponent})}>
      <label className="col-xs-3 control-label">
        Starting at
      </label>
      <div className="col-xs-4 schedule-values-container">
        {SelectComponent}
      </div>
    </div>
  );
};

StartingAtComponent.propTypes = {
  intervalOption: PropTypes.string
};

const ConnectedStartingAtComponent = connect(
  mapStateToStartingAtComponentProps,
  null
)(StartingAtComponent);


export default function StartingAt() {
  return (
    <ConnectedStartingAtComponent />
  );
}
