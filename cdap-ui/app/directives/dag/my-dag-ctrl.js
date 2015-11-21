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

angular.module(PKG.name + '.commons')
  .controller('MyDAGController', function MyDAGController(jsPlumb, $scope, $timeout, MyDAGFactory, GLOBALS, NodesActionsFactory, $window, NodesStore) {

    var vm = this;

    var endpoints = [];
    var sourceSettings = angular.copy(MyDAGFactory.getSettings(false).source);
    var sinkSettings = angular.copy(MyDAGFactory.getSettings(false).sink);
    var transformSourceSettings = angular.copy(MyDAGFactory.getSettings(false).transformSource);
    var transformSinkSettings = angular.copy(MyDAGFactory.getSettings(false).transformSink);

    var dragged = false;

    vm.scale = 1.0;


    function init() {
      $scope.nodes = NodesStore.getNodes();
      $scope.connections = NodesStore.getConnections();

      $timeout(function () {
        addEndpoints();

        angular.forEach($scope.connections, function (conn) {
          var sourceId = conn.source.indexOf('transform') !== -1 ? 'Left' + conn.source : conn.source;
          var targetId = conn.target.indexOf('transform') !== -1 ? 'Right' + conn.target : conn.target;
          vm.instance.connect({
            uuids: [sourceId, targetId]
          });
        });
      });
    }

    vm.zoomIn = function () {
      vm.scale += 0.1;
      setZoom(vm.scale, vm.instance);
    };

    vm.zoomOut = function () {
      vm.scale -= 0.1;
      setZoom(vm.scale, vm.instance);
    };


    /**
     * Utily function from jsPlumb
     * https://jsplumbtoolkit.com/community/doc/zooming.html
     **/
    function setZoom(zoom, instance, transformOrigin, el) {
      transformOrigin = transformOrigin || [ 0.5, 0.5 ];
      instance = instance || jsPlumb;
      el = el || instance.getContainer();
      var p = [ 'webkit', 'moz', 'ms', 'o' ],
          s = 'scale(' + zoom + ')',
          oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%';

      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s;
        el.style[p[i] + 'TransformOrigin'] = oString;
      }

      el.style['transform'] = s;
      el.style['transformOrigin'] = oString;

      instance.setZoom(zoom);
    }


    function addEndpoints() {
      angular.forEach($scope.nodes, function (node) {
        if (endpoints.indexOf(node.id) !== -1) {
          return;
        }
        endpoints.push(node.id);

        var type = GLOBALS.pluginConvert[node.type];
        switch(type) {
          case 'source':
            vm.instance.addEndpoint(node.id, sourceSettings, {uuid: node.id});
            break;
          case 'sink':
            vm.instance.addEndpoint(node.id, sinkSettings, {uuid: node.id});
            break;
          case 'transform':
            // Need to id each end point so that it can be used later to make connections.
            vm.instance.addEndpoint(node.id, transformSourceSettings, {uuid: 'Left' + node.id});
            vm.instance.addEndpoint(node.id, transformSinkSettings, {uuid: 'Right' + node.id});
            break;
        }
      });
    }

    function formatConnections() {
      var connections = [];
      angular.forEach(vm.instance.getConnections(), function (conn) {
        connections.push({
          source: conn.sourceId,
          target: conn.targetId
        });
      });
      NodesActionsFactory.setConnections(connections);
    }

    jsPlumb.ready(function() {
      var dagSettings = MyDAGFactory.getSettings().default;

      jsPlumb.setContainer('dag-container');
      vm.instance = jsPlumb.getInstance(dagSettings);

      init();

      vm.instance.bind('connection', formatConnections);
      vm.instance.bind('connectionDetached', formatConnections);

      $scope.$watchCollection('nodes', function () {
        $timeout(function () {
          var nodes = document.querySelectorAll('.box');
          vm.instance.draggable(nodes, {
            containment: true,
            start: function () { dragged = true; },
            stop: function () { $timeout(function () { vm.instance.repaintEverything(); }); }
          });
          addEndpoints();

        });
      });

      $scope.$watchCollection('connections', function () {
        console.log('ChangeConnection', $scope.connections);
      });

      // This is needed to redraw connections and endpoints on browser resize
      angular.element($window).on('resize', function() {
        vm.instance.repaintEverything();
      });
    });

    // var selectedNode = null;

    vm.clearNodeSelection = function () {
      vm.instance.clearDragSelection();
      angular.forEach($scope.nodes, function (node) {
        node.selected = false;
      });
    };

    function checkSelection() {
      vm.instance.clearDragSelection();

      var selected = [];
      angular.forEach($scope.nodes, function (node) {
        if (node.selected) {
          selected.push(node.id);
        }
      });

      vm.instance.addToDragSelection(selected);
    }

    vm.onNodeClick = function(event, node) {
      event.stopPropagation();

      if (dragged) {
        dragged = false;
        return;
      }

      if ((event.ctrlKey || event.metaKey)) {
        node.selected = !node.selected;
        NodesActionsFactory.resetSelectedNode();

        if (node.selected) {
          checkSelection();
        } else {
          vm.instance.removeFromDragSelection(node.id);
        }
      } else {
        vm.clearNodeSelection();
        node.selected = true;
        NodesActionsFactory.selectNode(node.id);
      }

      // $scope.nodeClick.call($scope.context, node);
    };

    vm.onNodeDelete = function (event, node) {
      event.stopPropagation();
      NodesActionsFactory.removeNode(node.id);
      vm.instance.remove(node.id);
    };


    $scope.$on('$destroy', function () {
      NodesActionsFactory.resetNodesAndConnections();
    });

  });
