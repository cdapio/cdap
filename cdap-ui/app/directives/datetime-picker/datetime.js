/*
 * Copyright © 2016 Cask Data, Inc.
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

function DatetimeController($scope) {
  'ngInject';

  var vm = this;
  vm.options = {
    initDate: new Date(),
    showWeeks: false,
    startingDay: 0
  };

  if ($scope.minDate) { vm.options.minDate = $scope.minDate; }
  if ($scope.maxDate) { vm.options.maxDate = $scope.maxDate; }

  $scope.dateObject = $scope.dateObject || new Date();

  function init () {
    vm.date = angular.copy($scope.dateObject);
    vm.hour = $scope.dateObject.getHours();
    vm.minutes = $scope.dateObject.getMinutes();
  }

  init();

  $scope.$watch('DatetimeController.hour', () => {
    vm.hour = parseInt(vm.hour, 10);
    formatDate();
  });
  $scope.$watch('DatetimeController.minutes', () => {
    vm.minutes = parseInt(vm.minutes, 10);
    formatDate();
  });
  $scope.$watch('DatetimeController.date', formatDate);

  function formatDate() {
    let year = vm.date.getFullYear(),
        month = vm.date.getMonth(),
        day = vm.date.getDate(),
        hour = vm.hour,
        minutes = vm.minutes;

    $scope.dateObject = (!hour && hour !== 0) || (!minutes && minutes !== 0) ? null : new Date(year, month, day, hour, minutes, 0);
  }

}

angular.module(PKG.name+'.commons')
  .directive('myDatetimePicker', () => {
    return {
      restrict: 'E',
      scope: {
        dateObject: '=',
        minDate: '=?',
        maxDate: '=?'
      },
      controller: DatetimeController,
      controllerAs: 'DatetimeController',
      templateUrl: 'datetime-picker/datetime.html'
    };
  });
