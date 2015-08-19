angular.module(PKG.name + '.feature.adapters')
  .controller('ScheduleController', function($scope, rAdapterDetail) {
    this.schedules = [];
    var schedule = {};
    var cronExpression;
    if (angular.isObject(rAdapterDetail.schedule)) {
      schedule = rAdapterDetail.schedule.schedule;
      schedule.status = 'SCHEDULED';
      schedule.scheduleType = 'TIME';
      schedule.isOpen = true;
      schedule.lastrun = 'UNAVAILABLE';

      cronExpression = schedule.cronExpression.split(' ');
      schedule.time = {};
      schedule.time.min = cronExpression[0];
      schedule.time.hour = cronExpression[1];
      schedule.time.day = cronExpression[2];
      schedule.time.month = cronExpression[3];
      schedule.time.week = cronExpression[4];

      this.schedules.push(schedule);
    }
  });
