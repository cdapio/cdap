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

  $scope.dateData = $scope.dateData || new Date();

  vm.min = 0;
  vm.max = 11;

  function init () {
    vm.date = angular.copy($scope.dateData);
    vm.hour = $scope.dateData.getHours();
    vm.isPM = false;

    if (vm.hour > 11) {
      vm.isPM = true;
      vm.hour = vm.hour % 12;
      vm.hour = vm.hour === 0 ? 12 : vm.hour;
      setMinMax(vm.isPM);
    }

    vm.minutes = $scope.dateData.getMinutes();
  }

  init();

  vm.togglePM = () => {
    vm.isPM = !vm.isPM;
    setMinMax(vm.isPM);
    formatDate();
  };

  function setMinMax(isPM) {
    if (isPM) {
      vm.min = 1;
      vm.max = 12;
    } else {
      vm.min = 0;
      vm.max = 11;
    }
  }

  $scope.$watch(() => {
    return vm.hour;
  }, () => {
    vm.hour = parseInt(vm.hour);
    formatDate();
  });
  $scope.$watch(() => {
    return vm.minutes;
  }, () => {
    vm.minutes = parseInt(vm.minutes);
    formatDate();
  });
  $scope.$watch(() => {
    return vm.date;
  }, formatDate);

  function formatDate() {
    let year = vm.date.getFullYear(),
        month = vm.date.getMonth(),
        day = vm.date.getDate(),
        hour = vm.isPM && vm.hour < 12 ? vm.hour + 12 : vm.hour,
        minutes = vm.minutes;

    $scope.dateData = new Date(year, month, day, hour, minutes, 0);
  }

}

angular.module(PKG.name+'.commons')
  .directive('myDatetimePicker', () => {
    return {
      restrict: 'E',
      scope: {
        dateData: '=',
        minDate: '=?',
        maxDate: '=?'
      },
      controller: DatetimeController,
      controllerAs: 'DatetimeController',
      templateUrl: 'datetime-picker/datetime.html'
    };
  });
