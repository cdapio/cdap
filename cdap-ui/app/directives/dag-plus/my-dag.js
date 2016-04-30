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

var commonModule = angular.module(PKG.name+'.commons');
// commonModule.factory('jsPlumb', function ($window) {
//   return $window.jsPlumb;
// });

commonModule.directive('myDagPlus', function() {
  return {
    restrict: 'E',
    scope: {
      isDisabled: '=',
      nodes: '=',
      connections: '=',
      nodeClick: '&',
      nodeDelete: '&',
      context: '=',
      templatePopover: '@',
      connectionPopoverData: '&',
      showMetrics: '=',
      metricsData: '=',
      nodePopoverTemplate: '@',
      disableNodeClick: '=',
      separation: '=?'
    },
    link: function(scope, element) {
      scope.element = element;
      scope.getGraphMargins = function (plugins) {
        var margins = this.element[0].parentElement.getBoundingClientRect();
        var parentWidth = margins.width;
        var parentHeight = margins.height;

        var scale = 1.0;

        // Find furthest nodes
        var maxLeft = 0;
        var maxTop = 0;
        angular.forEach(plugins, function (plugin) {
          if (!plugin._uiPosition) { return; }
          var left = parseInt(plugin._uiPosition.left, 10);
          var top = parseInt(plugin._uiPosition.top, 10);

          maxLeft = maxLeft < left ? left : maxLeft;
          maxTop = maxTop < top ? top : maxTop;
        });


        var marginLeft = (parentWidth - maxLeft) / 2 - 50;
        var marginTop = (parentHeight - maxTop) / 2 - 50;

        angular.forEach(plugins, function (plugin) {
          if (!plugin._uiPosition) { return; }
          var left = parseInt(plugin._uiPosition.left, 10) + marginLeft;
          var top = parseInt(plugin._uiPosition.top, 10) + marginTop;

          plugin._uiPosition.left = left + 'px';
          plugin._uiPosition.top = top + 'px';
        });


        if (maxLeft > parentWidth - 100) {
          scale = (parentWidth - 100) / maxLeft;
        }

        if (maxTop > parentHeight - 100) {
          var topScale = (parentHeight - 100) / maxTop;
          scale = scale < topScale ? scale : topScale;
        }

        return {
          scale: scale
        };
      };
    },
    templateUrl: 'dag-plus/my-dag.html',
    controller: 'DAGPlusPlusCtrl',
    controllerAs: 'DAGPlusPlusCtrl'
  };
});
