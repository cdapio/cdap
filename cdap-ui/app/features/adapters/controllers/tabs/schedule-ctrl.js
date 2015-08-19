angular.module(PKG.name + '.feature.adapters')
  .controller('ScheduleController', function($scope, rAdapterDetail) {
    this.schedules = [];
    var schedule = {};
    if (angular.isObject(rAdapterDetail.schedule)) {
      schedule = rAdapterDetail.schedule.schedule;
      schedule.status = 'SCHEDULED';
      schedule.scheduleType = 'TIME';
      schedule.isOpen = true;
      schedule.lastrun = 'UNAVAILABLE';
      this.schedules.push(schedule);
    }
  });
