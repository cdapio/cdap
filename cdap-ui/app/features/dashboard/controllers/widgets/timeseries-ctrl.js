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

// Probably should be renamed or refactored. This is not doing anything apart
// from handling size changes to the widget.
angular.module(PKG.name+'.feature.dashboard')
.controller('C3WidgetTimeseriesCtrl', function ($scope, myHelpers, $timeout) {
  $scope.chartSize = { height: 200 };
  var widget = myHelpers.objectQuery($scope, 'gridsterItem', '$element', 0),
      widgetHeight,
      widgetWidth;
  if (widget) {
    widgetHeight = parseInt(widget.style.height, 10);
    widgetWidth = parseInt(widget.style.width, 10);
    if (widgetHeight > 300) {
      $scope.wdgt.height = widgetHeight - 70;
    }
    $scope.wdgt.width = widgetWidth - 32;
  }

  $scope.$on('gridster-resized', function() {
    $timeout(function() {
      $scope.chartSize.height = parseInt($scope.gridsterItem.$element[0].style.height, 10) - 70;
      $scope.chartSize.width = parseInt($scope.gridsterItem.$element[0].style.width, 10) - 32;
    });
  });

  $scope.$watch('wdgt.height', function(newVal) {
    $scope.chartSize.height = newVal;
  });
  $scope.$watch('wdgt.width', function(newVal) {
    if (!newVal) {
      return;
    }
    $scope.chartSize.width = newVal;
  });

});
