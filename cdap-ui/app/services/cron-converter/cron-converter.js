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

angular.module(PKG.name + '.services')
  .factory('CronConverter', function(moment, myHelpers) {

    function convert(cron) {
      let cronValues = cron.split(' ');

      let cronObj = {
        'min': cronValues[0],
        'hour': cronValues[1],
        'day': cronValues[2],
        'month': cronValues[3],
        'daysOfWeek': cronValues[4]
      };

      let intervalOption = 'Hourly';
      let scheduleType = 'basic';

      let humanReadableObj = {
        'startingAt': {
          'min': '',
          'hour': '',
          'am_pm': ''
        },
        'repeatEvery': {
          'numMins': '',
          'numHours': '',
          'numDays': '',
          'daysOfWeek': [],
          'daysOfWeekObj': {},
          'dayOfMonth': '',
          'month': ''
        }
      };

      // Every 5 min
      if (cronValues[0] === '*/5' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = '5min';
        humanReadableObj.repeatEvery.numMins = 5;
      }

      // Every 10 min
      else if (cronValues[0] === '*/10' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = '10min';
        humanReadableObj.repeatEvery.numMins = 10;
      }

      // Every 30 min
      else if (cronValues[0] === '*/30' && cronValues[1] === '*' && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = '30min';
        humanReadableObj.repeatEvery.numMins = 30;
      }

      // Hourly
      else if (myHelpers.isNumeric(cronValues[0]) && cronValues[1].indexOf('/') !== -1 && cronValues[2] === '*' && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = 'Hourly';
        humanReadableObj.startingAt.min = parseInt(cronValues[0], 10);
        humanReadableObj.repeatEvery.numHours = parseInt(cronValues[1].split('/')[1], 10);
      }

      // Daily
      else if (myHelpers.isNumeric(cronValues[0]) && myHelpers.isNumeric(cronValues[1]) && cronValues[2].indexOf('/') !== -1 && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = 'Daily';
        humanReadableObj.startingAt.min = parseInt(cronValues[0], 10);
        let convertedHour = moment().hour(parseInt(cronValues[1]), 10);
        humanReadableObj.startingAt.hour = parseInt(convertedHour.format('h'), 10);
        humanReadableObj.startingAt.am_pm = convertedHour.format('A');
        humanReadableObj.repeatEvery.numDays = parseInt(cronValues[2].split('/')[1], 10);
      }

      // Weekly
      else if (myHelpers.isNumeric(cronValues[0]) && myHelpers.isNumeric(cronValues[1]) && cronValues[2] === '*' && cronValues[3] === '*' && (cronValues[4].indexOf(',') !== -1 || myHelpers.isNumeric(cronValues[4]))) {
        intervalOption = 'Weekly';
        humanReadableObj.startingAt.min = parseInt(cronValues[0], 10);
        let convertedHour = moment().hour(parseInt(cronValues[1]), 10);
        humanReadableObj.startingAt.hour = parseInt(convertedHour.format('h'), 10);
        humanReadableObj.startingAt.am_pm = convertedHour.format('A');
        humanReadableObj.repeatEvery.daysOfWeek = cronValues[4].split(',').map(val => parseInt(val), 10);
        humanReadableObj.repeatEvery.daysOfWeekObj = convertSelectedDaysToObj(humanReadableObj.repeatEvery.daysOfWeek);
      }

      // Monthly
      else if (myHelpers.isNumeric(cronValues[0]) && myHelpers.isNumeric(cronValues[1]) && myHelpers.isNumeric(cronValues[2]) && cronValues[3] === '*' && cronValues[4] === '*') {
        intervalOption = 'Monthly';
        humanReadableObj.startingAt.min = parseInt(cronValues[0], 10);
        let convertedHour = moment().hour(parseInt(cronValues[1]), 10);
        humanReadableObj.startingAt.hour = parseInt(convertedHour.format('h'), 10);
        humanReadableObj.startingAt.am_pm = convertedHour.format('A');
        humanReadableObj.repeatEvery.dayOfMonth = parseInt(cronValues[2], 10);
      }

      // Yearly
      else if (myHelpers.isNumeric(cronValues[0]) && myHelpers.isNumeric(cronValues[1]) && myHelpers.isNumeric(cronValues[2]) && myHelpers.isNumeric(cronValues[3]) && cronValues[4] === '*') {
        intervalOption = 'Yearly';
        humanReadableObj.startingAt.min = parseInt(cronValues[0], 10);
        let convertedHour = moment().hour(parseInt(cronValues[1]), 10);
        humanReadableObj.startingAt.hour = parseInt(convertedHour.format('h'), 10);
        humanReadableObj.startingAt.am_pm = convertedHour.format('A');
        humanReadableObj.repeatEvery.dayOfMonth = parseInt(cronValues[2], 10);
        humanReadableObj.repeatEvery.month = parseInt(cronValues[3], 10);
      }

      // Not handled in basic mode
      else {
        scheduleType = 'advanced';
      }

      return {
        cronObj: cronObj,
        intervalOption: intervalOption,
        humanReadableObj: humanReadableObj,
        scheduleType: scheduleType
      };
    }

    function convertSelectedDaysToObj(selectedDaysArr) {
      let checkboxObj = {};
      for (let i = 0; i < 7; i++) {
        if (selectedDaysArr.indexOf(i) !== -1) {
          checkboxObj[i] = true;
        } else {
          checkboxObj[i] = false;
        }
      }
      return checkboxObj;
    }

    return {
      convert: convert,
      convertSelectedDaysToObj: convertSelectedDaysToObj
    };
  });
