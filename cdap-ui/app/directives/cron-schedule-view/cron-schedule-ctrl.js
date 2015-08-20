angular.module(PKG.name + '.commons')
  .controller('CronScheduleViewController', function($scope) {
    var cronExpression = $scope.model.split(' ');
    $scope.schedule = {
      time: {}
    };
    $scope.schedule.time.min = cronExpression[0];
    $scope.schedule.time.hour = cronExpression[1];
    $scope.schedule.time.day = cronExpression[2];
    $scope.schedule.time.month = cronExpression[3];
    $scope.schedule.time.week = cronExpression[4];
  });
