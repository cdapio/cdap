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
commonModule.factory('jsPlumb', function ($window) {
  return $window.jsPlumb;
});

commonModule.directive('myDag', function() {
  return {
    restrict: 'E',
    scope: {
      config: '=',
      isDisabled: '='
    },
    link: function(scope, element) {
      scope.element = element;
      scope.getGraphMargins = function (plugins) {
        // Very simple logic for centering the DAG.
        // Should eventually be changed to something close to what we use in workflow/flow graphs.
        var margins = this.element[0].parentElement.getBoundingClientRect();
        var parentWidth = margins.width;
        var noOfNodes = plugins.length;
        var marginLeft = parentWidth - (noOfNodes * 174);
        if (marginLeft < 100){
          marginLeft = -20;
        } else {
          marginLeft = marginLeft/2;
        }
        return {
          left: marginLeft
        };
      };
    },
    templateUrl: 'dag/my-dag.html',
    controller: 'MyDAGController',
    controllerAs: 'MyDAGController'
  };
});
