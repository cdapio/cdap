angular.module(PKG.name + '.commons')
  .directive('mySchedule', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-schedule/widget-schedule.html',
      controller: function($scope, EventPipe) {
        var modelCopy = angular.copy($scope.model);

        var defaultSchedule = $scope.config.properties.default || ['*', '*', '*', '*', '*'];

        function initialize() {
          $scope.schedule = {};

          if (!$scope.model) {
            $scope.schedule.min = defaultSchedule[0];
            $scope.schedule.hour = defaultSchedule[1];
            $scope.schedule.day = defaultSchedule[2];
            $scope.schedule.month = defaultSchedule[3];
            $scope.schedule.week = defaultSchedule[4];
            return;
          }

          var initial = $scope.model.split(' ');

          $scope.schedule.min = initial[0];
          $scope.schedule.hour = initial[1];
          $scope.schedule.day = initial[2];
          $scope.schedule.month = initial[3];
          $scope.schedule.week = initial[4];
        }

        initialize();
        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);

          initialize();
        });

        $scope.$watch('schedule', function() {
          var schedule = '';
          angular.forEach($scope.schedule, function(v) {
            schedule += v + ' ';
          });
          schedule = schedule.trim();

          $scope.model = schedule;
        }, true);

      }
    };
  });
