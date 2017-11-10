/*
 * Copyright © 2017 Cask Data, Inc.
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
  .directive('mySplitterPopover', function() {
    return {
      restrict: 'A',
      scope: {
        node: '=',
        ports: '=',
        isDisabled: '=',
        onMetricsClick: '=',
        disableMetricsClick: '=',
        metricsData: '='
      },
      templateUrl: 'splitter-popover/splitter-popover.html',
      controller: function ($scope) {
        var vm = this;
        vm.ports = $scope.ports;

        const schemasAreDifferent = (newSchemas, oldSchemas) => {
          if (!newSchemas || !oldSchemas || newSchemas.length !== oldSchemas.length) {
            return true;
          }

          for (let i = 0; i < newSchemas.length; i++) {
            if (newSchemas[i].name !== oldSchemas[i].name) {
              return true;
            }
          }

          return false;
        };

        $scope.$watch('ports', (newValue, oldValue) => {
          if (schemasAreDifferent(newValue, oldValue)) {
            vm.ports = $scope.ports;
          }
        });
      },
      controllerAs: 'SplitterPopoverCtrl'
    };
  });
