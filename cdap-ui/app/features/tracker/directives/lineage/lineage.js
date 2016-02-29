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

function LineageController ($scope, jsPlumb, $timeout) {
  var vm = this;

  jsPlumb.ready( () => {
    vm.instance = jsPlumb.getInstance();
    jsPlumb.setContainer('lineage-diagram');

    $timeout( () => {
      angular.forEach($scope.connections, (conn) => {
        vm.instance.connect({
          source: conn.source,
          target: conn.target
        });
      });
    });
  });

}

LineageController.$inject = ['$scope', 'jsPlumb', '$timeout'];

angular.module(PKG.name + '.feature.tracker')
  .directive('myLineageDiagram', () => {
    return {
      restrict: 'E',
      scope: {
        nodes: '=',
        connections: '='
      },
      templateUrl: '/assets/features/tracker/directives/lineage/lineage.html',
      controller: LineageController,
      controllerAs: 'Lineage'
    };
  });