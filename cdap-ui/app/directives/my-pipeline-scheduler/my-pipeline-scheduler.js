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

class MyPipelineSchedulerCtrl {
  constructor(moment, myHelpers, CronConverter, $scope, myPipelineApi, $state, myAlertOnValium) {
    this.moment = moment;
    this._isDisabled = this.isDisabled === 'true';
    this.$scope = $scope;

    this.INTERVAL_OPTIONS = {
      '5min': 'Every 5 min',
      '10min': 'Every 10 min',
      '30min': 'Every 30 min',
      'Hourly': 'Hourly',
      'Daily': 'Daily',
      'Weekly': 'Weekly',
      'Monthly': 'Monthly',
      'Yearly': 'Yearly',
    };

    let minuteOptions = [];
    for (let i = 0; i < 60; i++) {
      minuteOptions.push(i);
    }

    this.myPipelineApi = myPipelineApi;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.MINUTE_OPTIONS = minuteOptions;
    this.HOUR_OPTIONS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23];
    this.HOUR_OPTIONS_CLOCK = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    this.DAY_OF_MONTH_OPTIONS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31];
    this.MONTH_OPTIONS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    this.AM_PM_OPTIONS = ['AM', 'PM'];

    this.initialCron = this.store.getSchedule();
    if (this.isDisabled) {
      this.scheduleStatus = this.store.getScheduleStatus();
    }
    this.cron = this.initialCron;

    this.intervalOptionKey = 'Hourly';
    this.isScheduleChange = false;
    this.savingSchedule = false;

    let defaulTimeOptions = {
      'startingAt': {
        'min': this.MINUTE_OPTIONS[0],
        'hour': this.HOUR_OPTIONS[0],
        'am_pm': this.AM_PM_OPTIONS[0]
      },
      'repeatEvery': {
        'numMins': 5,
        'numHours': this.HOUR_OPTIONS_CLOCK[0],
        'numDays': this.DAY_OF_MONTH_OPTIONS[0],
        'daysOfWeek': [1],
        'daysOfWeekObj': CronConverter.convertSelectedDaysToObj([1]),
        'dayOfMonth': this.DAY_OF_MONTH_OPTIONS[0],
        'month': this.MONTH_OPTIONS[0]
      }
    };

    this.timeSelections = Object.assign({}, defaulTimeOptions);

    this.advancedScheduleValues = {
      'min': 0,
      'hour': '*',
      'day': '*',
      'month': '*',
      'daysOfWeek': '*'
    };

    this.scheduleType = 'basic';

    // '0 * * * *' is default cron
    if (this.cron !== '0 * * * *') {
      let convertedCronValues = CronConverter.convert(this.cron);
      this.advancedScheduleValues = convertedCronValues.cronObj;
      this.intervalOptionKey = convertedCronValues.intervalOption;
      this.timeSelections = convertedCronValues.humanReadableObj;
      this.scheduleType = convertedCronValues.scheduleType;
    }

    this.switchToDefault = () => {
      this.timeSelections = Object.assign({}, defaulTimeOptions);
      this.updateCron();
    };

    this.suspendScheduleAndClose = () => {
      this.suspendSchedule();
      this.onClose();
    };

    this.startScheduleAndClose = () => {
      if (this.isScheduleChange) {
        this.saveAndStartSchedule();
      }
      this.startSchedule();
    };
    this.saveAndStartSchedule = () => {
      this.saveSchedule()
        .then(
          () => {
            this.startSchedule()
              .then(() => {
                this.$state.reload();
                this.onClose();
              });
          },
          (err) => {
            this.myAlertOnValium.show({
              type: 'danger',
              content: typeof err === 'object' ? err.data : 'Updating pipeline failed: ' + err
            });
          }
        );
    };

    this.saveSchedule = () => {
      this.getUpdatedCron();
      if (!this.isDisabled) {
        this.actionCreator.setSchedule(this.cron);
        this.onClose();
        return;
      }
      this.savingSchedule = true;
      let pipelineConfig = this.store.getCloneConfig();
      pipelineConfig.config.schedule = this.cron;
      return this.myPipelineApi.save(
        {
          namespace: this.$state.params.namespace,
          pipeline: pipelineConfig.name
        },
        pipelineConfig
      )
        .$promise
        .then(
          () => {
            this.savingSchedule = false;
            this.onClose();
          },
          (err) => {
            this.savingSchedule = false;
            this.myAlertOnValium.show({
              type: 'danger',
              content: typeof err === 'object' ? err.data : 'Updating pipeline failed: ' + err
            });
          }
        );
    };

    this.selectType = (type) => {
      this.scheduleType = type;
    };

    this.updateCron = () => {
      if (!this.isDisabled) {
        return;
      }
      let cron = this.getUpdatedCron();
      if (cron !== this.initialCron) {
        this.isScheduleChange = true;
      }
    };
    this.updateSelectedDaysInWeek = () => {
      for (let key in this.timeSelections.repeatEvery.daysOfWeekObj) {
        if (this.timeSelections.repeatEvery.daysOfWeekObj.hasOwnProperty(key)) {
          let keyInt = parseInt(key, 10);
          if (this.timeSelections.repeatEvery.daysOfWeekObj[key] && this.timeSelections.repeatEvery.daysOfWeek.indexOf(keyInt) === -1) {
            this.timeSelections.repeatEvery.daysOfWeek.push(keyInt);
          } else if (!this.timeSelections.repeatEvery.daysOfWeekObj[key] && this.timeSelections.repeatEvery.daysOfWeek.indexOf(keyInt) !== -1) {
            this.timeSelections.repeatEvery.daysOfWeek.splice(this.timeSelections.repeatEvery.daysOfWeek.indexOf(keyInt));
          }
        }
      }
      this.timeSelections.repeatEvery.daysOfWeek.sort();
      this.updateCron();
    };

    this.getUpdatedCron = () => {
      if (this.scheduleType === 'basic') {
        let convertedHour;
        switch (this.intervalOptionKey) {
          case '5min':
            this.cron = '*/5 * * * *';
            break;
          case '10min':
            this.cron = '*/10 * * * *';
            break;
          case '30min':
            this.cron = '*/30 * * * *';
            break;
          case 'Hourly':
            this.cron = `${this.timeSelections.startingAt.min} */${this.timeSelections.repeatEvery.numHours} * * *`;
            break;
          case 'Daily':
            convertedHour = this.moment(this.timeSelections.startingAt.hour.toString() + this.timeSelections.startingAt.am_pm, 'hA').format('H');
            this.cron = `${this.timeSelections.startingAt.min} ${convertedHour} */${this.timeSelections.repeatEvery.numDays} * *`;
            break;
          case 'Weekly':
            convertedHour = this.moment(this.timeSelections.startingAt.hour.toString() + this.timeSelections.startingAt.am_pm, 'hA').format('H');
            this.cron = `${this.timeSelections.startingAt.min} ${convertedHour} * * ${this.timeSelections.repeatEvery.daysOfWeek.toString()}`;
            break;
          case 'Monthly':
            convertedHour = this.moment(this.timeSelections.startingAt.hour.toString() + this.timeSelections.startingAt.am_pm, 'hA').format('H');
            this.cron = `${this.timeSelections.startingAt.min} ${convertedHour} ${this.timeSelections.repeatEvery.dayOfMonth} * *`;
            break;
          case 'Yearly':
            convertedHour = this.moment(this.timeSelections.startingAt.hour.toString() + this.timeSelections.startingAt.am_pm, 'hA').format('H');
            this.cron = `${this.timeSelections.startingAt.min} ${convertedHour} ${this.timeSelections.repeatEvery.dayOfMonth} ${this.timeSelections.repeatEvery.month} *`;
            break;
        }
      } else {
        this.cron = `${this.advancedScheduleValues.min} ${this.advancedScheduleValues.hour} ${this.advancedScheduleValues.day} ${this.advancedScheduleValues.month} ${this.advancedScheduleValues.daysOfWeek}`;
      }
    };
  }
}

MyPipelineSchedulerCtrl.$inject = ['moment', 'myHelpers', 'CronConverter', '$scope', 'myPipelineApi', '$state', 'myAlertOnValium'];
  angular.module(PKG.name + '.commons')
  .controller('MyPipelineSchedulerCtrl', MyPipelineSchedulerCtrl)
  .filter('cronDayOfMonth', () => {
    return (input) => {
      switch (input) {
        case 1:
          return '1st';
        case 2:
          return '2nd';
        case 3:
          return '3rd';
        case 21:
          return '21st';
        case 22:
          return '22nd';
        case 23:
          return '23rd';
        case 31:
          return '31st';
        case null:
          return null;
        default:
          return input + 'th';
      }
    };
  })
  .filter('cronMonthName', () => {
    return (input) => {
      let months = {
        1: 'Jan',
        2: 'Feb',
        3: 'Mar',
        4: 'Apr',
        5: 'May',
        6: 'Jun',
        7: 'Jul',
        8: 'Aug',
        9: 'Sep',
        10: 'Oct',
        11: 'Nov',
        12: 'Dec'
      };

      if (input !== null && angular.isDefined(months[input])) {
        return months[input];
      } else {
        return null;
      }
    };
  })
  .filter('cronDayName', () => {
    return (input) => {
      let days = {
        0: 'Sunday',
        1: 'Monday',
        2: 'Tuesday',
        3: 'Wednesday',
        4: 'Thursday',
        5: 'Friday',
        6: 'Saturday',
      };

      if (input !== null && angular.isDefined(days[input])) {
        return days[input];
      } else {
        return null;
      }
    };
  })
  .filter('getFirstLetter', () => {
    return (input) => {
      if (input !== null && input.length > 0) {
        return input.charAt(0);
      } else {
        return null;
      }
    };
  })
  .filter('displayTwoDigitNumber', () => {
    return (number) => {
      if (number !== null && !isNaN(number)) {
        return ('0' + number).slice(-2);
      } else {
        return null;
      }
    };
  })
  .directive('myPipelineScheduler', function() {
    return {
      restrict: 'E',
      scope: {
        store: '=',
        actionCreator: '=',
        pipelineName: '@',
        onClose: '&',
        startSchedule: '&',
        isDisabled: '@',
        suspendSchedule: '&'
      },
      bindToController: true,
      controller: 'MyPipelineSchedulerCtrl',
      controllerAs: 'SchedulerCtrl',
      templateUrl: 'my-pipeline-scheduler/my-pipeline-scheduler.html'
    };
  });
