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

import moment from 'moment';
import {isNumeric} from 'services/helpers';
import {wholeArrayIsNumeric} from 'services/helpers';
import PipelineSchedulerStore, {
  INTERVAL_OPTIONS,
  SCHEDULE_VIEWS,
  DEFAULT_SCHEDULE_OPTIONS,
  ACTIONS as PipelineSchedulerActions
} from 'components/PipelineScheduler/Store';
import {getCurrentNamespace} from 'services/NamespaceStore';
import PipelineDetailStore from 'components/PipelineDetails/store';
import {MyScheduleApi} from 'api/schedule';
import {GLOBALS} from 'services/global-constants';

function setStateFromCron(cron = PipelineSchedulerStore.getState().cron) {
  let cronValues = cron.split(' ');
  let payload = {};
  let converted12HourFormat = moment().hour(parseInt(cronValues[1]), 10);

  // Every 5 min
  if (cronValues[0] === '*/5' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS['5MIN'],
      minInterval: 5
    };
  }

  // Every 10 min
  else if (cronValues[0] === '*/10' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS['10MIN'],
      minInterval: 10
    };
  }

  // Every 30 min
  else if (cronValues[0] === '*/30' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS['30MIN'],
      minInterval: 30
    };
  }

  // Hourly
  else if (isNumeric(cronValues[0]) && cronValues[1].indexOf('/') !== -1 && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS.HOURLY,
      startingAtMinute: parseInt(cronValues[0], 10),
      hourInterval: parseInt(cronValues[1].split('/')[1], 10)
    };
  }

  // Daily
  else if (wholeArrayIsNumeric(cronValues.slice(0, 2)) && cronValues[2].indexOf('/') !== -1 && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS.DAILY,
      startingAtMinute: parseInt(cronValues[0], 10),
      startingAtHour: parseInt(converted12HourFormat.format('h'), 10),
      startingAtAMPM: converted12HourFormat.format('A'),
      dayInterval: parseInt(cronValues[2].split('/')[1], 10)
    };
  }

  // Weekly
  else if (wholeArrayIsNumeric(cronValues.slice(0, 2)) && cronValues[2] === '*' && cronValues[3] === '*' && (cronValues[4].indexOf(',') !== -1 || isNumeric(cronValues[4]))) {
    payload = {
      intervalOption: INTERVAL_OPTIONS.WEEKLY,
      startingAtMinute: parseInt(cronValues[0], 10),
      startingAtHour: parseInt(converted12HourFormat.format('h'), 10),
      startingAtAMPM: converted12HourFormat.format('A'),
      daysOfWeekInterval: cronValues[4].split(',').map(val => parseInt(val), 10)
    };
  }

  // Monthly
  else if (wholeArrayIsNumeric(cronValues.slice(0, 3)) && cronValues[3] === '*' && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS.MONTHLY,
      startingAtMinute: parseInt(cronValues[0], 10),
      startingAtHour: parseInt(converted12HourFormat.format('h'), 10),
      startingAtAMPM: converted12HourFormat.format('A'),
      dateOfMonthInterval: parseInt(cronValues[2], 10)
    };
  }

  // Yearly
  else if (wholeArrayIsNumeric(cronValues.slice(0, 4)) && cronValues[4] === '*') {
    payload = {
      intervalOption: INTERVAL_OPTIONS.YEARLY,
      startingAtMinute: parseInt(cronValues[0], 10),
      startingAtHour: parseInt(converted12HourFormat.format('h'), 10),
      startingAtAMPM: converted12HourFormat.format('A'),
      dateOfMonthInterval: parseInt(cronValues[2], 10),
      monthInterval: parseInt(cronValues[3], 10) - 1
    };
  }

  // Not handled in basic mode
  else if (cron !== DEFAULT_SCHEDULE_OPTIONS.cron) {
    payload = {
      scheduleView: SCHEDULE_VIEWS.ADVANCED
    };
  }

  PipelineSchedulerStore.dispatch({
    type: PipelineSchedulerActions.SET_STATE,
    payload: {
      ...payload,
      cron
    }
  });
}

function getCronFromState() {
  let state = PipelineSchedulerStore.getState();
  let converted24HourFormat = moment(state.startingAtHour.toString() + state.startingAtAMPM, 'hA').format('H');
  let cron;
  switch (state.intervalOption) {
    case INTERVAL_OPTIONS['5MIN']:
      cron = '*/5 * * * *';
      break;
    case INTERVAL_OPTIONS['10MIN']:
      cron = '*/10 * * * *';
      break;
    case INTERVAL_OPTIONS['30MIN']:
      cron = '*/30 * * * *';
      break;
    case INTERVAL_OPTIONS.HOURLY:
      cron = `${state.startingAtMinute} */${state.hourInterval} * * *`;
      break;
    case INTERVAL_OPTIONS.DAILY:
      cron = `${state.startingAtMinute} ${converted24HourFormat} */${state.dayInterval} * *`;
      break;
    case INTERVAL_OPTIONS.WEEKLY:
      cron = `${state.startingAtMinute} ${converted24HourFormat} * * ${state.daysOfWeekInterval.toString()}`;
      break;
    case INTERVAL_OPTIONS.MONTHLY:
      cron = `${state.startingAtMinute} ${converted24HourFormat} ${state.dateOfMonthInterval} * *`;
      break;
    case INTERVAL_OPTIONS.YEARLY:
      cron = `${state.startingAtMinute} ${converted24HourFormat} ${state.dateOfMonthInterval} ${moment().month(state.monthInterval).format('M')} *`;
      break;
  }

  return cron;
}

function updateCron() {
  let cron = getCronFromState();
  PipelineSchedulerStore.dispatch({
    type: PipelineSchedulerActions.SET_CRON,
    payload: {
      cron
    }
  });
}

function setSelectedProfile(selectedProfile, profileCustomizations = {}) {
  PipelineSchedulerStore.dispatch({
    type: PipelineSchedulerActions.SET_SELECTED_PROFILE,
    payload: {
      selectedProfile,
      profileCustomizations
    }
  });
}

function getTimeBasedSchedule() {
  let {name: appId} = PipelineDetailStore.getState();
  MyScheduleApi
    .get({
      namespace: getCurrentNamespace(),
      appId,
      scheduleName: GLOBALS.defaultScheduleId
    })
    .subscribe(
      (currentBackendSchedule) => {
        PipelineSchedulerStore.dispatch({
          type: PipelineSchedulerActions.SET_CURRENT_BACKEND_SCHEDULE,
          payload: {
            currentBackendSchedule
          }
        });
        setStateFromCron();
      },
      (err) => {
        console.log('Failed to fetch dataPipelineSchedule Schedule from backend: ', err);
      }
    );
}

function setScheduleStatus(scheduleStatus) {
  PipelineSchedulerStore.dispatch({
    type: PipelineSchedulerActions.SET_SCHEDULE_STATUS,
    payload: {
      scheduleStatus
    }
  });
}

export {
  setStateFromCron,
  setSelectedProfile,
  getTimeBasedSchedule,
  getCronFromState,
  updateCron,
  setScheduleStatus
};
