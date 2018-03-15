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
import {INTERVAL_OPTIONS, HOUR_OPTIONS, DAY_OF_WEEK_OPTIONS, DATE_OF_MONTH_OPTIONS, MONTH_OPTIONS, ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';
import {updateCron} from 'components/PipelineScheduler/Store/ActionCreator';
import {connect} from 'react-redux';
import classnames from 'classnames';
import moment from 'moment';
import {Input} from 'reactstrap';
import T from 'i18n-react';

const PREFIX = 'features.PipelineScheduler.repeatEvery';

const mapStateToHourIntervalProps = (state) => {
  return {
    options: HOUR_OPTIONS,
    value: state.hourInterval
  };
};
const mapDispatchToHourIntervalProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_HOUR_INTERVAL,
        payload: {
          hourInterval: e.target.value
        }
      });
      updateCron();
    }
  };
};

const mapStateToDayIntervalProps = (state) => {
  return {
    options: DATE_OF_MONTH_OPTIONS,
    value: state.dayInterval
  };
};
const mapDispatchToDayIntervalProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_DAY_INTERVAL,
        payload: {
          dayInterval: e.target.value
        }
      });
      updateCron();
    }
  };
};

const mapStateToDaysOfWeekIntervalProps = (state, ownProps) => {
  return {
    type: 'checkbox',
    checked: state.daysOfWeekInterval.indexOf(ownProps.option) !== -1
  };
};
const mapDispatchToDaysOfWeekIntervalProps = (dispatch, ownProps) => {
  return {
    onChange: () => {
      dispatch({
        type: PipelineSchedulerActions.SET_DAYS_OF_WEEK_INTERVAL,
        payload: {
          dayOption: ownProps.option
        }
      });
      updateCron();
    }
  };
};

const mapStateToDateOfMonthIntervalProps = (state) => {
  return {
    options: DATE_OF_MONTH_OPTIONS,
    value: state.dateOfMonthInterval
  };
};
const mapDispatchToDateOfMonthIntervalProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_DATE_OF_MONTH_INTERVAL,
        payload: {
          dateOfMonthInterval: e.target.value
        }
      });
      updateCron();
    }
  };
};

const mapStateToMonthIntervalProps = (state) => {
  return {
    options: MONTH_OPTIONS.map(monthNum => moment().month(monthNum).format('MMM')),
    value: moment().month(state.monthInterval).format('MMM')
  };
};
const mapDispatchToMonthIntervalProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineSchedulerActions.SET_MONTH_INTERVAL,
        payload: {
          monthInterval: moment().month(e.target.value).format('M') - 1
        }
      });
      updateCron();
    }
  };
};

const SelectHourInterval = connect(
  mapStateToHourIntervalProps,
  mapDispatchToHourIntervalProps
)(SelectWithOptions);
const SelectDayInterval = connect(
  mapStateToDayIntervalProps,
  mapDispatchToDayIntervalProps
)(SelectWithOptions);
const SelectDaysOfWeekCheckbox = connect(
  mapStateToDaysOfWeekIntervalProps,
  mapDispatchToDaysOfWeekIntervalProps
)(Input);
const SelectDateOfMonthInterval = connect(
  mapStateToDateOfMonthIntervalProps,
  mapDispatchToDateOfMonthIntervalProps
)(SelectWithOptions);
const SelectMonthInterval = connect(
  mapStateToMonthIntervalProps,
  mapDispatchToMonthIntervalProps
)(SelectWithOptions);

const mapStateToRepeatEveryComponentProps = (state) => {
  return {
    intervalOption: state.intervalOption
  };
};

const RepeatEveryComponent = ({intervalOption}) => {
  let SelectComponent;

  switch (intervalOption) {
    case INTERVAL_OPTIONS.HOURLY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectHourInterval className="form-control small-dropdown" />
          <span>{T.translate(`${PREFIX}.hour`)}</span>
        </span>
      );
      break;
    case INTERVAL_OPTIONS.DAILY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectDayInterval className="form-control small-dropdown" />
          <span>{T.translate(`${PREFIX}.day`)}</span>
        </span>
      );
      break;
    case INTERVAL_OPTIONS.WEEKLY:
      SelectComponent = (
        <span className="schedule-values">
          {
            DAY_OF_WEEK_OPTIONS.map(option => {
              return (
                <div className="day-of-week">
                  <label>
                    <SelectDaysOfWeekCheckbox
                      option={option}
                    />
                    {/* need to do -1 because our backend expects Sun-Sat values as 1-7, but moment() expects 0-6 instead */}
                    <span>{moment().day(parseInt(option, 10)-1).format('ddd')}</span>
                  </label>
                </div>
              );
            })
          }
        </span>
      );
      break;
    case INTERVAL_OPTIONS.MONTHLY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectDateOfMonthInterval className="form-control small-dropdown" />
          <span>{T.translate(`${PREFIX}.dayOfMonth`)}</span>
        </span>
      );
      break;
    case INTERVAL_OPTIONS.YEARLY:
      SelectComponent = (
        <span className="schedule-values">
          <SelectMonthInterval className="form-control small-dropdown" />
          <SelectDateOfMonthInterval className="form-control small-dropdown" />
        </span>
      );
      break;
  }

  let shouldHideComponent = [INTERVAL_OPTIONS['5MIN'], INTERVAL_OPTIONS['10MIN'], INTERVAL_OPTIONS['30MIN']].indexOf(intervalOption) !== -1;

  return (
    <div className={classnames('form-group row', {'invisible': shouldHideComponent})}>
      <label className="col-xs-3 control-label">
        {T.translate(`${PREFIX}.label`)}
      </label>
      <div className="col-xs-4 schedule-values-container">
        {SelectComponent}
      </div>
    </div>
  );
};

RepeatEveryComponent.propTypes = {
  intervalOption: PropTypes.string
};

const ConnectedRepeatEveryComponent = connect(
  mapStateToRepeatEveryComponentProps,
  null
)(RepeatEveryComponent);


export default function RepeatEvery() {
  return (
    <ConnectedRepeatEveryComponent />
  );
}
