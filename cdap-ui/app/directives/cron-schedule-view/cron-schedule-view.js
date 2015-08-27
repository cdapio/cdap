angular.module(PKG.name + '.commons')
  .directive('myCronScheduleView', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
      },
      templateUrl: 'cron-schedule-view/cron-schedule-view.html',
      controller: 'CronScheduleViewController'
    };
  });
