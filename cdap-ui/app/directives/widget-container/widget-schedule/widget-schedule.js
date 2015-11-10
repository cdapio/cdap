/*
 * Copyright © 2015 Cask Data, Inc.
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
