angular.module(PKG.name + '.commons')
  .directive('mySchedulesView', function() {
    return {
      restrict: 'EA',
      scope: {
        schedules: '=',
        isSchedulesDisabled: '@',
        onScheduleAction: '&',
        scheduleContext: '='
      },
      templateUrl: 'schedules-view/schedules-view.html',
      controller: function($scope) {
        $scope.disableSchedule = ($scope.isSchedulesDisabled === 'true');
        $scope.takeScheduleAction = function(schedule, action) {

            var fn = $scope.onScheduleAction();
            if ('undefined' !== typeof fn) {
              fn.call($scope.scheduleContext, schedule, action);
            }

        };
      }
    };
  });
