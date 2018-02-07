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

/*
  This store represents the state of the Pipeline Schedule modeless.

  In Studio view, all the configs will have default values, while in Detail view
  this store is initialized using values from the pipeline json/PipelineDetailStore

  When the user makes a change inside the modeless and clicks Save, then we dispatch
  an action in PipelineScheduler component to save the new configs to config-store.js
  (in Studio view) or to PipelineDetailStore (in Detail view)
*/

import {defaultAction, composeEnhancers} from 'services/helpers';
import {createStore} from 'redux';
import range from 'lodash/range';
import {HYDRATOR_DEFAULT_VALUES} from 'services/global-constants';

const INTERVAL_OPTIONS = {
  '5MIN': 'Every 5 min',
  '10MIN': 'Every 10 min',
  '30MIN': 'Every 30 min',
  'HOURLY': 'Hourly',
  'DAILY': 'Daily',
  'WEEKLY': 'Weekly',
  'MONTHLY': 'Monthly',
  'YEARLY': 'Yearly',
};

const DAY_OF_WEEK_OPTIONS = range(1, 8);
const MINUTE_OPTIONS = range(0, 60);
const HOUR_OPTIONS = range(1, 24);
const HOUR_OPTIONS_CLOCK = range(1, 13);
const DATE_OF_MONTH_OPTIONS = range(1, 32);
const MONTH_OPTIONS = range(0, 12);
const AM_PM_OPTIONS = ['AM', 'PM'];
const MAX_CONCURRENT_RUNS_OPTIONS = range(1, 11);
const SCHEDULE_VIEWS = {
  'BASIC': 'basic',
  'ADVANCED': 'advanced'
};

const ACTIONS = {
  SET_CRON: 'SET_CRON',
  UPDATE_CRON: 'UPDATE_CRON',
  SET_STATE: 'SET_STATE',
  SET_INTERVAL_OPTION: 'SET_INTERVAL_OPTION',
  SET_MINUTE_INTERVAL: 'SET_MINUTE_INTERVAL',
  SET_HOUR_INTERVAL: 'SET_HOUR_INTERVAL',
  SET_DAY_INTERVAL: 'SET_DAY_INTERVAL',
  SET_DAYS_OF_WEEK_INTERVAL: 'SET_DAYS_OF_WEEK_INTERVAL',
  SET_DATE_OF_MONTH_INTERVAL: 'SET_DATE_OF_MONTH_INTERVAL',
  SET_MONTH_INTERVAL: 'SET_MONTH_INTERVAL',
  SET_STARTING_AT_MINUTE: 'SET_STARTING_AT_MINUTE',
  SET_STARTING_AT_HOUR: 'SET_STARTING_AT_HOUR',
  SET_STARTING_AT_AM_PM: 'SET_STARTING_AT_AM_PM',
  SET_MAX_CONCURRENT_RUNS: 'SET_MAXCURRENT_RUNS',
  SET_SCHEDULE_VIEW: 'SET_SCHEDULE_VIEW',
  RESET: 'RESET'
};

const DEFAULT_SCHEDULE_OPTIONS = {
  cron: HYDRATOR_DEFAULT_VALUES.schedule,
  intervalOption: INTERVAL_OPTIONS.DAILY,
  minInterval: 5,
  hourInterval: HOUR_OPTIONS_CLOCK[0],
  dayInterval: DATE_OF_MONTH_OPTIONS[0],
  daysOfWeekInterval: [1],
  dateOfMonthInterval: DATE_OF_MONTH_OPTIONS[0],
  monthInterval: MONTH_OPTIONS[0],
  startingAtMinute: MINUTE_OPTIONS[0],
  startingAtHour: HOUR_OPTIONS[0],
  startingAtAMPM: AM_PM_OPTIONS[0],
  maxConcurrentRuns: MAX_CONCURRENT_RUNS_OPTIONS[0],
  scheduleView: Object.values(SCHEDULE_VIEWS)[0]
};

const schedule = (state = DEFAULT_SCHEDULE_OPTIONS, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SET_CRON:
      return {
        ...state,
        cron: action.payload.cron
      };
    case ACTIONS.UPDATE_CRON: {
      let cronArray = state.cron.split(" ");
      cronArray[action.payload.index] = action.payload.value || '0';

      return {
        ...state,
        cron: cronArray.join(" ")
      };
    }
    case ACTIONS.SET_STATE:
      return {
        ...state,
        ...action.payload
      };
    case ACTIONS.SET_INTERVAL_OPTION:
      return {
        ...state,
        intervalOption: action.payload.intervalOption
      };
    case ACTIONS.SET_MINUTE_INTERVAL:
      return {
        ...state,
        minInterval: parseInt(action.payload.minInterval, 10)
      };
    case ACTIONS.SET_HOUR_INTERVAL:
      return {
        ...state,
        hourInterval: parseInt(action.payload.hourInterval, 10)
      };
    case ACTIONS.SET_DAY_INTERVAL:
      return {
        ...state,
        dayInterval: parseInt(action.payload.dayInterval, 10)
      };
    case ACTIONS.SET_DAYS_OF_WEEK_INTERVAL: {
      let daysOfWeekInterval = [...state.daysOfWeekInterval];
      let dayOption = action.payload.dayOption;
      let dayOptionIndex = daysOfWeekInterval.indexOf(dayOption);
      if (dayOptionIndex !== -1) {
        daysOfWeekInterval.splice(dayOptionIndex, 1);
      } else {
        daysOfWeekInterval.push(dayOption);
      }
      daysOfWeekInterval.sort();
      return {
        ...state,
        daysOfWeekInterval
      };
    }
    case ACTIONS.SET_DATE_OF_MONTH_INTERVAL:
      return {
        ...state,
        dateOfMonthInterval: parseInt(action.payload.dateOfMonthInterval, 10)
      };
    case ACTIONS.SET_MONTH_INTERVAL:
      return {
        ...state,
        monthInterval: parseInt(action.payload.monthInterval, 10)
      };
    case ACTIONS.SET_STARTING_AT_MINUTE:
      return {
        ...state,
        startingAtMinute: parseInt(action.payload.startingAtMinute, 10)
      };
    case ACTIONS.SET_STARTING_AT_HOUR:
      return {
        ...state,
        startingAtHour: parseInt(action.payload.startingAtHour, 10)
      };
    case ACTIONS.SET_STARTING_AT_AM_PM:
      return {
        ...state,
        startingAtAMPM: action.payload.startingAtAMPM
      };
    case ACTIONS.SET_MAX_CONCURRENT_RUNS:
      return {
        ...state,
        maxConcurrentRuns: parseInt(action.payload.maxConcurrentRuns, 10)
      };
    case ACTIONS.SET_SCHEDULE_VIEW:
      return {
        ...state,
        scheduleView: action.payload.scheduleView
      };
    case ACTIONS.RESET:
      return DEFAULT_SCHEDULE_OPTIONS;
  }
};

const PipelineSchedulerStore = createStore(
  schedule,
  DEFAULT_SCHEDULE_OPTIONS,
  composeEnhancers('PipelineSchedulerStore')()
);

export default PipelineSchedulerStore;
export {
  INTERVAL_OPTIONS,
  MINUTE_OPTIONS,
  HOUR_OPTIONS,
  HOUR_OPTIONS_CLOCK,
  DAY_OF_WEEK_OPTIONS,
  DATE_OF_MONTH_OPTIONS,
  MONTH_OPTIONS,
  AM_PM_OPTIONS,
  MAX_CONCURRENT_RUNS_OPTIONS,
  SCHEDULE_VIEWS,
  ACTIONS,
  DEFAULT_SCHEDULE_OPTIONS
};
