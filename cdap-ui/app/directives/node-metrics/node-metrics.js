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

angular.module(PKG.name+'.commons')
.directive('myNodeMetrics', function ($timeout) {
  return {
    restrict: 'E',
    templateUrl: 'node-metrics/node-metrics.html',
    scope: {
      onClick: '&',
      node: '=',
      metricsData: '=',
      disabled: '=',
      portName: '='
    },
    controller: function($scope) {
      $scope.showLabels = true;
      $scope.timeout = null;
      $scope.$watch('metricsData', function () {
        $scope.timeout = $timeout(() => {
          let nodeElem = document.getElementById($scope.node.name);
          let nodeMetricsElem = nodeElem.querySelector(`.metrics-content`);
          if (nodeMetricsElem && nodeMetricsElem.offsetWidth < nodeMetricsElem.scrollWidth) {
            $scope.showLabels = false;
          }
        });
      }, true);
      $scope.$on('$destroy', () => {
        if ($scope.timeout) {
          $timeout.cancel($scope.timeout);
        }
      });
    },
    link: function(scope) {
      scope.onClick = scope.onClick();
    }
  };
});
