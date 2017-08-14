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
      metricsData: '='
    },
    link: function(scope) {
      let metricsTimeout = null;

      scope.onClick = scope.onClick();

      scope.$watch('metricsData', function () {
        angular.forEach(scope.metricsData, function (value, key) {
          metricsTimeout = $timeout(function () {
            let nodeElem = document.getElementById(key);
            let nodeMetricsElem = nodeElem.querySelector(`.metrics-content`);
            if (nodeMetricsElem.offsetWidth < nodeMetricsElem.scrollWidth) {
              let recordsOutLabelElem = nodeMetricsElem.querySelector('.metric-records-out-label');
              if (recordsOutLabelElem) {
                recordsOutLabelElem.parentNode.removeChild(recordsOutLabelElem);
              }
              let errorsLabelElem = nodeMetricsElem.querySelector('.metric-errors-label');
              if (errorsLabelElem) {
                errorsLabelElem.parentNode.removeChild(errorsLabelElem);
              }
            }
          });
        });
      }, true);

      scope.$on('$destroy', function () {
        $timeout.cancel(metricsTimeout);
      });
    }
  };
});
