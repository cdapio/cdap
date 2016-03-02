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

function LineageController ($scope, jsPlumb, $timeout, $state, LineageStore) {
  var vm = this;

  function render() {
    vm.nodes = LineageStore.getNodes();
    vm.connections = LineageStore.getConnections();
    vm.uniqueNodes = LineageStore.getUniqueNodes();
    vm.graph = LineageStore.getGraph();


    vm.graphInfo = vm.graph.graph();

    $timeout( () => {
      angular.forEach(vm.connections, (conn) => {
        vm.instance.connect({
          source: conn.source,
          target: conn.target,
          detachable: false,
          anchors: ['Right', 'Left']
        });
      });

      $timeout( () => {
        vm.instance.repaintEverything();
      });
    });
  }

  LineageStore.registerOnChangeListener(render);

  jsPlumb.ready( () => {
    jsPlumb.setContainer('lineage-diagram');

    vm.instance = jsPlumb.getInstance({
      PaintStyle: {
        lineWidth: 2,
        strokeStyle: 'rgba(0,0,0, 1)'
      },
      Connector: [ 'Flowchart', {gap: 0, stub: [10, 15], alwaysRespectStubs: true, cornerRadius: 3} ],
      Endpoints: ['Blank', 'Blank']
    });

    render();
  });

  vm.nodeClick = (node) => {
    let nodeInfo = vm.uniqueNodes[node.uniqueNodeId];
    if (nodeInfo.nodeType === 'data') {
      $state.go('tracker.entity.metadata', { entityType: nodeInfo.entityType, entityId: nodeInfo.entityId });
    }
  };

  vm.mouseLeaveNode = (node) => {
    node.timeout = $timeout(() => {
      node.showNavigation = false;
    }, 300);
  };

  vm.mouseEnterNavigation = (node) => {
    $timeout.cancel(node.timeout);
  };

  vm.mouseLeaveNavigation = (node) => {
    $timeout(() => {
      node.showNavigation = false;
    }, 300);
  };

  vm.navigationClick = (event, node) => {
    event.stopPropagation();
    let unique = vm.uniqueNodes[node.uniqueNodeId];
    $scope.navigationFunction().call($scope.context, unique.entityType, unique.entityId);
  };

}

LineageController.$inject = ['$scope', 'jsPlumb', '$timeout', '$state', 'LineageStore'];

angular.module(PKG.name + '.feature.tracker')
  .directive('myLineageDiagram', () => {
    return {
      restrict: 'E',
      scope: {
        nodes: '=',
        connections: '=',
        graph: '=',
        uniqueNodes: '=',
        navigationFunction: '&',
        context: '='
      },
      templateUrl: '/assets/features/tracker/directives/lineage/lineage.html',
      controller: LineageController,
      controllerAs: 'Lineage'
    };
  });
