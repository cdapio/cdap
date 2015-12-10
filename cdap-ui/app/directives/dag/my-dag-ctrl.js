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
  .controller('MyDAGController', function MyDAGController(jsPlumb, $scope, $timeout, MyDAGFactory, GLOBALS, NodesActionsFactory, $window, NodesStore, HydratorErrorFactory, $rootScope, HydratorService, $popover, $filter) {

    var vm = this;

    var numberFilter = $filter('number');

    var endpoints = [];
    var sourceSettings = angular.copy(MyDAGFactory.getSettings(false).source);
    var sinkSettings = angular.copy(MyDAGFactory.getSettings(false).sink);
    var transformSourceSettings = angular.copy(MyDAGFactory.getSettings(false).transformSource);
    var transformSinkSettings = angular.copy(MyDAGFactory.getSettings(false).transformSink);

    var SHOW_METRICS_THRESHOLD = 0.8;

    var labels = [];

    var metricsLabel = [
      [ 'Custom', {
        create: function (label) {
          labels.push(label);
          return angular.element('<span></span>');
        },
        location: [2, 0],
        id: 'metricLabel'
      }]
    ];

    if ($scope.showMetrics) {
      sourceSettings.overlays = metricsLabel;
      transformSourceSettings.overlays = metricsLabel;
    }

    var dragged = false;
    var canvasDragged = false;

    vm.isDisabled = $scope.isDisabled;

    var popovers = [];
    var nodePopovers = {};

    vm.scale = 1.0;

    vm.panning = {
      style: {
        'top': 0,
        'left': 0
      },
      top: 0,
      left: 0
    };

    /*
    FIXME: This should be fixed. Right now the assumption is to update
     the store before my-dag directive is rendered. What happens if we get the
     data after the rendering? The init function is never called or the DAG is not
     rendered based on the data.

     Right now there is a cycle which prevents us from listening to the NodeStore
     changes. The infinite recurrsion happens like this,
     Assuming we have this
     NodesStore.registerChangeListener(init);
     1. User adds a connection in view
     2. 'connection' event is fired by jsplumb
     3. On connection event we call 'formatConnections' function
     4. 'formatConnections' constructs the 'connections' array and sets it to NodesStore
     5. Now NodesStore fires an update to changelisteners.
     6. 'init' function gets called.
     6. 'init' function again programmatically connects all the nodes and sets it to NodesStore
     7. Control goes to step 5
     7. Hence the loop.

     We need to be able to separate render of graph from data and incremental user interactions.
     - Programmatically it should be possible to provide data and should be able to ask dag to render it at any time post-rendering of the directive
     - User should be able interact with the dag and add incremental changes.
    */
    function init() {
      $scope.nodes = NodesStore.getNodes();
      $scope.connections = NodesStore.getConnections();

      $timeout(function () {
        // centering DAG
        if ($scope.nodes.length) {
          var margins = $scope.getGraphMargins($scope.nodes);
          $timeout(function () { vm.instance.repaintEverything(); });

          vm.scale = margins.scale;
        }

        addEndpoints();

        angular.forEach($scope.connections, function (conn) {
          var sourceNode = $scope.nodes.filter( node => node.id === conn.from);
          var targetNode = $scope.nodes.filter( node => node.id === conn.to);
          if (!sourceNode.length || !targetNode.length) {
            return;
          }
          var sourceId = sourceNode[0].type === 'transform' ? 'Left' + conn.from : conn.from;
          var targetId = targetNode[0].type === 'transform' ? 'Right' + conn.to : conn.to;
          var connObj = {
            uuids: [sourceId, targetId]
          };
          if (vm.isDisabled) {
            connObj.detachable = false;
          }

          vm.instance.connect(connObj);
        });

        setZoom(vm.scale, vm.instance);

        // Process metrics data
        if ($scope.showMetrics) {

          angular.forEach($scope.nodes, function (node) {
            var elem = angular.element(document.getElementById(node.id)).children();

            var scope = $rootScope.$new();

            scope.data = {
              nodeName: node.label
            };

            nodePopovers[node.id] = {
              scope: scope,
              element: elem,
              popover: null,
              isShowing: false
            };

            $scope.$on('$destroy', function () {
              scope.$destroy();
            });

          });

          if (vm.scale <= SHOW_METRICS_THRESHOLD) {
            hideMetricsLabel();
          }

          $scope.$watch('metricsData', function () {
            angular.forEach($scope.metricsData, function (value, key) {
              nodePopovers[key].scope.data.metrics = value;
            });

            angular.forEach(labels, function (endpoint) {
              var label = endpoint.getOverlay('metricLabel');
              if ($scope.metricsData[endpoint.elementId] === null || $scope.metricsData[endpoint.elementId] === undefined) {
                return;
              }

              angular.element(label.getElement())
                .text(numberFilter($scope.metricsData[endpoint.elementId].recordsOut, 0));
            });
          }, true);
        }

      });
    }

    vm.nodeMouseEnter = _.throttle(function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }
      var nodeInfo = nodePopovers[node.id];

      nodeInfo.popover = $popover(nodeInfo.element, {
        trigger: 'manual',
        placement: 'auto right',
        target: angular.element(nodeInfo.element[0]),
        templateUrl: $scope.nodePopoverTemplate,
        container: 'main',
        scope: nodeInfo.scope
      });
      nodeInfo.popover.$promise
        .then(function () {
          $timeout(function () {
            nodeInfo.popover.show();
          });
        });

    }, 300);

    vm.nodeMouseLeave = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      var nodeInfo = nodePopovers[node.id];
      if (!nodeInfo.popover) { return; }

      nodeInfo.popover.hide();
      nodeInfo.popover.destroy();
      nodeInfo.popover = null;
    };

    vm.zoomIn = function () {
      closeAllPopovers();
      vm.scale += 0.1;

      if (vm.scale >= SHOW_METRICS_THRESHOLD) {
        showMetricsLabel();
      }

      setZoom(vm.scale, vm.instance);
    };

    vm.zoomOut = function () {
      closeAllPopovers();
      if (vm.scale <= 0.2) { return; }

      if (vm.scale <= SHOW_METRICS_THRESHOLD) {
        hideMetricsLabel();
      }

      vm.scale -= 0.1;
      setZoom(vm.scale, vm.instance);
    };

    function showMetricsLabel() {
      angular.forEach(labels, function (label) {
        label.getOverlay('metricLabel').show();
      });
    }

    function hideMetricsLabel() {
      angular.forEach(labels, function (label) {
        label.getOverlay('metricLabel').hide();
      });
    }


    /**
     * Utily function from jsPlumb
     * https://jsplumbtoolkit.com/community/doc/zooming.html
     *
     * slightly modified to fit our needs
     **/
    function setZoom(zoom, instance, transformOrigin, el) {
      if ($scope.nodes.length === 0) { return; }

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

    function transformCanvas (top, left) {
      vm.panning.top += top;
      vm.panning.left += left;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };
    }

    function formatConnections() {
      closeAllPopovers();
      var connections = [];
      angular.forEach(vm.instance.getConnections(), function (conn) {
        connections.push({
          from: conn.sourceId,
          to: conn.targetId
        });
      });
      NodesActionsFactory.setConnections(connections);
    }

    function addConnection (connectionObj) {
      var connection = connectionObj.connection;

      var label = angular.element(connection.getOverlay('label').getElement());
      var scope = $rootScope.$new();

      var popover = $popover(label, {
        trigger: 'manual',
        placement: 'auto',
        target: label,
        templateUrl: $scope.templatePopover,
        container: 'main',
        scope: scope
      });

      popovers.push(popover);

      connection.bind('click', function (conn, event) {
        event.stopPropagation();
        scope.data = $scope.connectionPopoverData().call($scope.context, connection.sourceId, connection.targetId);
        popover.show();
      });

      $scope.$on('$destroy', function () {
        scope.$destroy();
      });

      formatConnections();
    }

    function closeAllPopovers() {
      if (popovers.length === 0) { return; }

      angular.forEach(popovers, function (popover) {
        popover.hide();
      });
    }

    jsPlumb.ready(function() {
      var dagSettings = MyDAGFactory.getSettings().default;

      jsPlumb.setContainer('dag-container');
      vm.instance = jsPlumb.getInstance(dagSettings);

      init();

      // Making canvas draggable
      vm.secondInstance = jsPlumb.getInstance();
      vm.secondInstance.draggable('diagram-container', {
        stop: function (e) {
          e.el.style.left = '0px';
          e.el.style.top = '0px';
          transformCanvas(e.pos[1], e.pos[0]);
          NodesActionsFactory.resetPluginCount();
          NodesActionsFactory.setCanvasPanning(vm.panning);
        },
        start: function () {
          canvasDragged = true;
          closeAllPopovers();
        }
      });

      vm.instance.bind('connection', addConnection);
      vm.instance.bind('connectionDetached', formatConnections);



      // This should be removed once the node config is using FLUX
      $scope.$watch('nodes', function () {
        closeAllPopovers();

        $timeout(function () {
          var nodes = document.querySelectorAll('.box');
          addEndpoints();

          if (!vm.isDisabled) {
            vm.instance.draggable(nodes, {
              start: function () {
                dragged = true;
                closeAllPopovers();
              },
              stop: function (dragEndEvent) {
                var config = {
                  _uiPosition: {
                    top: dragEndEvent.el.style.top,
                    left: dragEndEvent.el.style.left
                  }
                };
                NodesActionsFactory.updateNode(dragEndEvent.el.id, config);
                $timeout(function () { vm.instance.repaintEverything(); });
              }
            });
          }
        });

        angular.forEach($scope.nodes, function (plugin) {
          plugin.requiredFieldCount = HydratorErrorFactory.countRequiredFields(plugin);
          if (plugin.requiredFieldCount > 0) {
            plugin.error = {
              message: GLOBALS.en.hydrator.studio.genericMissingRequiredFieldsError
            };
          } else {
            plugin.error = false;
          }
        });

      }, true);

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
      if (canvasDragged) {
        canvasDragged = false;
        return;
      }
      closeAllPopovers();
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
      closeAllPopovers();
      NodesActionsFactory.removeNode(node.id);
      vm.instance.remove(node.id);
    };

    vm.cleanUpGraph = function () {
      if ($scope.nodes.length === 0) { return; }

      var graphNodes = MyDAGFactory.getGraphLayout($scope.nodes, $scope.connections)._nodes;

      angular.forEach($scope.nodes, function (node) {
        var location = graphNodes[node.id];
        node._uiPosition = {
          left: location.x + 'px',
          top: location.y + 'px'
        };
      });

      vm.panning.top = 0;
      vm.panning.left = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      var margins = $scope.getGraphMargins($scope.nodes);
      vm.scale = margins.scale;
      $timeout(function () { vm.instance.repaintEverything(); });
      setZoom(vm.scale, vm.instance);

      NodesActionsFactory.resetPluginCount();
      NodesActionsFactory.setCanvasPanning(vm.panning);
    };

    vm.locateNodes = function () {
      var minLeft = null;
      var leftMostNode = null;

      angular.forEach($scope.nodes, function (node) {
        var left = parseInt(node._uiPosition.left, 10);

        if (node._uiPosition.left.includes('vw')) {
          left = parseInt(left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }

        if (minLeft === null || left < minLeft) {
          minLeft = left;
          leftMostNode = node;
        }
      });

      var offsetLeft = parseInt(leftMostNode._uiPosition.left, 10);
      var offsetTop = parseInt(leftMostNode._uiPosition.top, 10);

      angular.forEach($scope.nodes, function (node) {
        var left = parseInt(node._uiPosition.left, 10);
        var top = parseInt(node._uiPosition.top, 10);

        node._uiPosition = {
          left: (left - offsetLeft + 50) + 'px',
          top: (top - offsetTop + 150) + 'px'
        };
      });

      $timeout(function () { vm.instance.repaintEverything(); });

      vm.panning.top = 0;
      vm.panning.left = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      NodesActionsFactory.resetPluginCount();
      NodesActionsFactory.setCanvasPanning(vm.panning);
    };


    $scope.$on('$destroy', function () {
      closeAllPopovers();
      NodesActionsFactory.resetNodesAndConnections();
      NodesStore.reset();
    });

  });
