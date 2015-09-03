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
