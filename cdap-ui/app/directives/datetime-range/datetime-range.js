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

function RangeController ($scope) {
  'ngInject';

  var vm = this;

  vm.startTimeOpen = false;
  vm.endTimeOpen = false;

  vm.openStartTime = () => {
    vm.startTimeOpen = true;
    vm.endTimeOpen = false;

    document.body.removeEventListener('click', vm.close, false);
    document.body.addEventListener('click', vm.close, false);
  };

  vm.openEndTime = () => {
    vm.startTimeOpen = false;
    vm.endTimeOpen = true;

    document.body.removeEventListener('click', vm.close, false);
    document.body.addEventListener('click', vm.close, false);
  };

  vm.done = () => {
    vm.close();

    $scope.onDone();
  };

  vm.close = () => {
    vm.startTimeOpen = false;
    vm.endTimeOpen = false;

    document.body.removeEventListener('click', vm.close, false);
  };

  let keydownListener = (event) => {
    if (event.keyCode !== 27) { return; }
    vm.close();
  };

  document.addEventListener('keydown', keydownListener);

  $scope.$on('$destroy', () => {
    document.removeEventListener('keydown', keydownListener);
    document.body.removeEventListener('click', vm.close, false);
  });
}

angular.module(PKG.name+'.commons')
  .directive('myDatetimeRange', () => {
    return {
      restrict: 'E',
      scope: {
        dateRange: '=',
        onDone: '&'
      },
      controller: RangeController,
      controllerAs: 'RangeController',
      templateUrl: 'datetime-range/datetime-range.html'
    };
  });
