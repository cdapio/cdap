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
  .controller('DAGPlusPlusCtrl', function MyDAGController(jsPlumb, $scope, $timeout, DAGPlusPlusFactory, GLOBALS, DAGPlusPlusNodesActionsFactory, $window, DAGPlusPlusNodesStore, $rootScope, $popover, $filter, uuid, $tooltip) {

    var vm = this;

    var numberFilter = $filter('number');

    var endpoints = [];

    var settings = DAGPlusPlusFactory.getSettings();

    var sourceOrigin = settings.sourceOrigin,
        sourceTarget = settings.sourceTarget,
        transformOrigin = settings.transformOrigin,
        transformTarget = settings.transformTarget,
        sinkOrigin = settings.sinkOrigin,
        sinkTarget = settings.sinkTarget,
        actionOrigin = settings.actionOrigin,
        actionTarget = settings.actionTarget;

    let localX,localY;

    var SHOW_METRICS_THRESHOLD = 0.8;
    var METRICS_THRESHOLD = 999999999999;
    var selected = [];
    var labels = [];

    var separation = $scope.separation || 200; // node separation length

    var metricsLabel = [
      [ 'Custom', {
        create: function (label) {
          labels.push(label);
          return angular.element('<div><span class="metric-label-text"></span></div>');
        },
        width: 100,
        location: [4.3, 0],
        id: 'metricLabel',
        cssClass: 'metric-label'
      }]
    ];

    if ($scope.showMetrics) {
      sourceOrigin.overlays = metricsLabel;
      transformOrigin.overlays = metricsLabel;
    }

    var dragged = false;
    var canvasDragged = false;

    vm.isDisabled = $scope.isDisabled;
    vm.disableNodeClick = $scope.disableNodeClick;

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

    vm.comments = [];

    var repaintTimeout = null,
        commentsTimeout = null,
        nodesTimeout = null,
        fitToScreenTimeout = null,
        initTimeout = null,
        nodePopoverTimeout = null;

    function repaintEverything() {
      if (repaintTimeout) {
        $timeout.cancel(repaintTimeout);
      }

      repaintTimeout = $timeout(function () { vm.instance.repaintEverything(); });
    }


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
     - User should be able interact with the dag and  incremental changes.
    */
    function init() {
      $scope.nodes = DAGPlusPlusNodesStore.getNodes();
      $scope.connections = DAGPlusPlusNodesStore.getConnections();
      vm.comments = DAGPlusPlusNodesStore.getComments();

      initTimeout = $timeout(function () {
        addEndpoints();

        angular.forEach($scope.connections, function (conn) {
          var sourceNode = $scope.nodes.filter( node => node.name === conn.from);
          var targetNode = $scope.nodes.filter( node => node.name === conn.to);
          if (!sourceNode.length || !targetNode.length) {
            return;
          }

          var sourceId = 'Origin' + conn.from;
          var targetId = 'Target' + conn.to;

          var connObj = {
            uuids: [sourceId, targetId]
          };

          vm.instance.connect(connObj);
        });

        if (vm.isDisabled) {
          // Disable all endpoints
          angular.forEach(endpoints, function (node) {
            var endpointArr = vm.instance.getEndpoints(node);

            // There should only be 2 endpoints per nodes, left and right
            angular.forEach(endpointArr, function (endpoint) {
              endpoint.setEnabled(false);
            });
          });
        }

        // Process metrics data
        if ($scope.showMetrics) {

          angular.forEach($scope.nodes, function (node) {
            var elem = angular.element(document.getElementById(node.name)).children();

            var scope = $rootScope.$new();
            scope.data = {
              nodeName: node.name
            };

            nodePopovers[node.name] = {
              scope: scope,
              element: elem,
              popover: null,
              isShowing: false
            };

            $scope.$on('$destroy', function () {
              elem.remove();
              elem = null;
              scope.$destroy();
            });

          });

          if (vm.scale <= SHOW_METRICS_THRESHOLD) {
            hideMetricsLabel();
          }

          angular.forEach(labels, function (endpoint) {
            var label = endpoint.getOverlay('metricLabel');

            var tooltip = $tooltip(angular.element(label.getElement()).children(), {
              trigger: 'hover',
              title: 'Records Out',
              delay: 300,
              container: 'body'
            });

            $scope.$on('$destroy', function () {
              tooltip.destroy();
            });

          });

          $scope.$watch('metricsData', function () {
            if (Object.keys($scope.metricsData).length === 0) {
              angular.forEach(nodePopovers, function (value) {
                value.scope.data.metrics = 0;
              });
            }

            angular.forEach($scope.metricsData, function (value, key) {
              nodePopovers[key].scope.data.metrics = value;
            });

            angular.forEach(labels, function (endpoint) {
              var label = endpoint.getOverlay('metricLabel');
              if ($scope.metricsData[endpoint.elementId] === null || $scope.metricsData[endpoint.elementId] === undefined) {
                angular.element(label.getElement()).children()
                  .text(0);
                return;
              }

              var recordsOut = $scope.metricsData[endpoint.elementId].recordsOut;

              // hide label if the metric is greater than METRICS_THRESHOLD.
              // the intent is to hide the metrics when the length is greater than 12.
              // Since records out metrics is an integer we can do a straight comparison
              if(recordsOut > METRICS_THRESHOLD) {
                label.hide();
              } else if (recordsOut <= METRICS_THRESHOLD && vm.scale >= SHOW_METRICS_THRESHOLD ) {
                label.show();
              }

              angular.element(label.getElement()).children()
                .text(numberFilter(recordsOut, 0));

            });
          }, true);
        }
      });

      // This is here because the left panel is initially in the minimized mode and expands
      // based on user setting on local storage. This is taking more than a single angular digest cycle
      // Hence the timeout to 1sec to render it in subsequent digest cycles.
      // FIXME: This directive should not be dependent on specific external component to render itself.
      // The left panel should default to expanded view and cleaning up the graph and fit to screen should happen in parallel.
      fitToScreenTimeout = $timeout(() => {
        vm.fitToScreen();
      }, 500);
    }

    vm.nodeMouseEnter = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }
      var nodeInfo = nodePopovers[node.name];

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
          if (nodePopoverTimeout) {
            $timeout.cancel(nodePopoverTimeout);
          }

          nodePopoverTimeout = $timeout(function () {
            nodeInfo.popover.show();
          });
        });
    };

    vm.nodeMouseLeave = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      var nodeInfo = nodePopovers[node.name];
      if (!nodeInfo.popover) { return; }

      nodeInfo.popover.hide();
      nodeInfo.popover.destroy();
      nodeInfo.popover = null;
    };

    vm.zoomIn = function () {
      vm.scale += 0.1;

      if (vm.scale >= SHOW_METRICS_THRESHOLD) {
        showMetricsLabel();
      }

      setZoom(vm.scale, vm.instance);
    };

    vm.zoomOut = function () {
      if (vm.scale <= 0.2) { return; }

      if (vm.scale <= SHOW_METRICS_THRESHOLD) {
        hideMetricsLabel();
      }

      vm.scale -= 0.1;
      setZoom(vm.scale, vm.instance);
    };

    function showMetricsLabel() {
      angular.forEach(labels, function (label) {
        if ($scope.metricsData[label.elementId] && $scope.metricsData[label.elementId].recordsOut > METRICS_THRESHOLD) {
          return;
        }

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
        if (endpoints.indexOf(node.name) !== -1) {
          return;
        }
        endpoints.push(node.name);

        var type = GLOBALS.pluginConvert[node.type];

        switch(type) {
          case 'source':
            vm.instance.addEndpoint(node.name, sourceOrigin, {uuid: 'Origin' + node.name});
            vm.instance.addEndpoint(node.name, sourceTarget, {uuid: 'Target' + node.name});
            break;
          case 'sink':
            vm.instance.addEndpoint(node.name, sinkOrigin, {uuid: 'Origin' + node.name});
            vm.instance.addEndpoint(node.name, sinkTarget, {uuid: 'Target' + node.name});
            break;
          case 'action':
            vm.instance.addEndpoint(node.name, actionOrigin, {uuid: 'Origin' + node.name});
            vm.instance.addEndpoint(node.name, actionTarget, {uuid: 'Target' + node.name});
            break;
          default:
            // Need to id each end point so that it can be used later to make connections.
            var originId = {uuid: 'Origin' + node.name};
            var targetId = {uuid: 'Target' + node.name};
            if (node.plugin.name === 'Wrangler') {
              originId.cssClass = 'wrangler-anchor';
              targetId.cssClass = 'wrangler-anchor';
            }

            vm.instance.addEndpoint(node.name, transformOrigin, originId);
            vm.instance.addEndpoint(node.name, transformTarget, targetId);
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
      var connections = [];
      angular.forEach(vm.instance.getConnections(), function (conn) {
        connections.push({
          from: conn.sourceId,
          to: conn.targetId
        });
      });
      DAGPlusPlusNodesActionsFactory.setConnections(connections);
    }

    jsPlumb.ready(function() {
      var dagSettings = DAGPlusPlusFactory.getSettings().default;

      jsPlumb.setContainer('dag-container');
      vm.instance = jsPlumb.getInstance(dagSettings);

      init();

      // Making canvas draggable
      vm.secondInstance = jsPlumb.getInstance();
      if (!vm.disableNodeClick) {
        vm.secondInstance.draggable('diagram-container', {
          stop: function (e) {
            e.el.style.left = '0px';
            e.el.style.top = '0px';
            transformCanvas(e.pos[1], e.pos[0]);
            DAGPlusPlusNodesActionsFactory.resetPluginCount();
            DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
          },
          start: function () {
            canvasDragged = true;
          }
        });
      }
      vm.instance.bind('connection', formatConnections);
      vm.instance.bind('connectionDetached', formatConnections);



      // This should be removed once the node config is using FLUX
      $scope.$watch('nodes', function () {
        if (nodesTimeout) {
          $timeout.cancel(nodesTimeout);
        }
        nodesTimeout = $timeout(function () {
          var nodes = document.querySelectorAll('.box');
          addEndpoints();

          if (!vm.isDisabled) {
            vm.instance.draggable(nodes, {
              start: function (drag) {
                let currentCoOrdinates = {
                  x: drag.e.clientX,
                  y: drag.e.clientY,
                };
                if (currentCoOrdinates.x === localX && currentCoOrdinates.y === localY) {
                  return;
                }
                localX = currentCoOrdinates.x;
                localY = currentCoOrdinates.y;
                if (selected.indexOf(drag.el.id) === -1) {
                  vm.clearNodeSelection();
                }

                dragged = true;
              },
              stop: function (dragEndEvent) {
                var config = {
                  _uiPosition: {
                    top: dragEndEvent.el.style.top,
                    left: dragEndEvent.el.style.left
                  }
                };
                DAGPlusPlusNodesActionsFactory.updateNode(dragEndEvent.el.id, config);
                repaintEverything();
              }
            });
          }
        });
      }, true);
      // This is needed to redraw connections and endpoints on browser resize
      angular.element($window).on('resize', vm.instance.repaintEverything);

      DAGPlusPlusNodesStore.registerOnChangeListener(function () {
        vm.comments = DAGPlusPlusNodesStore.getComments();

        if (!vm.isDisabled) {
          if (commentsTimeout) {
            $timeout.cancel(commentsTimeout);
          }

          commentsTimeout = $timeout(function () {
            var comments = document.querySelectorAll('.comment-box');
            vm.instance.draggable(comments, {
              start: function () {
                dragged = true;
              },
              stop: function (dragEndEvent) {
                var config = {
                  _uiPosition: {
                    top: dragEndEvent.el.style.top,
                    left: dragEndEvent.el.style.left
                  }
                };
                DAGPlusPlusNodesActionsFactory.updateComment(dragEndEvent.el.id, config);
              }
            });
          });
        }
      });

    });

    vm.clearNodeSelection = function () {
      if (canvasDragged) {
        canvasDragged = false;
        return;
      }
      selected = [];
      vm.instance.clearDragSelection();
      DAGPlusPlusNodesActionsFactory.resetSelectedNode();
      angular.forEach($scope.nodes, function (node) {
        node.selected = false;
      });
      clearCommentSelection();
    };

    function checkSelection() {
      vm.instance.clearDragSelection();

      selected = [];
      angular.forEach($scope.nodes, function (node) {
        if (node.selected) {
          selected.push(node.name);
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
        DAGPlusPlusNodesActionsFactory.resetSelectedNode();

        if (node.selected) {
          checkSelection();
        } else {
          vm.instance.removeFromDragSelection(node.name);
        }
      } else {
        vm.clearNodeSelection();
        node.selected = true;
        DAGPlusPlusNodesActionsFactory.selectNode(node.name);
      }
    };

    vm.onNodeDelete = function (event, node) {
      event.stopPropagation();
      DAGPlusPlusNodesActionsFactory.removeNode(node.name);
      vm.instance.remove(node.name);
    };

    vm.cleanUpGraph = function () {
      if ($scope.nodes.length === 0) { return; }

      var graphNodes = DAGPlusPlusFactory.getGraphLayout($scope.nodes, $scope.connections, separation)._nodes;
      angular.forEach($scope.nodes, function (node) {
        var location = graphNodes[node.name];
        node._uiPosition = {
          left: location.x - 50 + 'px',
          top: location.y + 'px'
        };
      });

      $scope.getGraphMargins($scope.nodes);

      vm.panning.top = 0;
      vm.panning.left = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      repaintEverything();

      DAGPlusPlusNodesActionsFactory.resetPluginCount();
      DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
    };

    // This algorithm is f* up
    vm.fitToScreen = function () {
      if ($scope.nodes.length === 0) { return; }

      /**
       * Need to find the furthest nodes:
       * 1. Left most nodes
       * 2. Right most nodes
       * 3. Top most nodes
       * 4. Bottom most nodes
       **/
      var minLeft = _.min($scope.nodes, function (node) {
        if (node._uiPosition.left.includes('vw')) {
          var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }
        return parseInt(node._uiPosition.left, 10);
      });
      var maxLeft = _.max($scope.nodes, function (node) {
        if (node._uiPosition.left.includes('vw')) {
          var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }
        return parseInt(node._uiPosition.left, 10);
      });

      var minTop = _.min($scope.nodes, function (node) {
        return parseInt(node._uiPosition.top, 10);
      });

      var maxTop = _.max($scope.nodes, function (node) {
        return parseInt(node._uiPosition.top, 10);
      });

      /**
       * Calculate the max width and height of the actual diagram by calculating the difference
       * between the furthest nodes + margins ( 50 on each side ).
       **/
      var width = parseInt(maxLeft._uiPosition.left, 10) - parseInt(minLeft._uiPosition.left, 10) + 100;
      var height = parseInt(maxTop._uiPosition.top, 10) - parseInt(minTop._uiPosition.top, 10) + 100;

      var parent = $scope.element[0].parentElement.getBoundingClientRect();

      // calculating the scales and finding the minimum scale
      var widthScale = (parent.width - 150) / width;
      var heightScale = (parent.height - 100) / height;

      vm.scale = Math.min(widthScale, heightScale);

      if (vm.scale > 1) {
        vm.scale = 1;
      }
      setZoom(vm.scale, vm.instance);


      // This will move all nodes by the minimum left and minimum top by the container
      // with margin of 50px
      var offsetLeft = parseInt(minLeft._uiPosition.left, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.left = (parseInt(node._uiPosition.left, 10) - offsetLeft) + 'px';
      });

      var offsetTop = parseInt(minTop._uiPosition.top, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.top = (parseInt(node._uiPosition.top, 10) - offsetTop + 50) + 'px';
      });

      $scope.getGraphMargins($scope.nodes);

      repaintEverything();

      vm.panning.left = 0;
      vm.panning.top = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      DAGPlusPlusNodesActionsFactory.resetPluginCount();
      DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
    };

    vm.addComment = function () {
      var canvasPanning = DAGPlusPlusNodesStore.getCanvasPanning();

      var config = {
        content: '',
        isActive: false,
        id: 'comment-' + uuid.v4(),
        _uiPosition: {
          'top': 250 - canvasPanning.top + 'px',
          'left': (10/100 * document.documentElement.clientWidth) - canvasPanning.left + 'px'
        }
      };

      DAGPlusPlusNodesActionsFactory.addComment(config);
    };

    function clearCommentSelection() {
      angular.forEach(vm.comments, function (comment) {
        comment.isActive = false;
      });
    }

    vm.commentSelect = function (event, comment) {
      event.stopPropagation();
      clearCommentSelection();

      if (dragged) {
        dragged = false;
        return;
      }

      comment.isActive = true;
    };

    vm.deleteComment = function (comment) {
      DAGPlusPlusNodesActionsFactory.deleteComment(comment);
    };

    $scope.$on('$destroy', function () {
      labels = [];
      DAGPlusPlusNodesActionsFactory.resetNodesAndConnections();
      DAGPlusPlusNodesStore.reset();

      angular.element($window).off('resize', vm.instance.repaintEverything);

      // Cancelling all timeouts
      $timeout.cancel(repaintTimeout);
      $timeout.cancel(commentsTimeout);
      $timeout.cancel(nodesTimeout);
      $timeout.cancel(fitToScreenTimeout);
      $timeout.cancel(initTimeout);
      $timeout.cancel(nodePopoverTimeout);

    });

  });
