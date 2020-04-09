/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
  .controller('DAGPlusPlusCtrl', function MyDAGController(jsPlumb, $scope, $timeout, DAGPlusPlusFactory, GLOBALS, DAGPlusPlusNodesActionsFactory, $window, DAGPlusPlusNodesStore, $rootScope, $modifiedPopover, uuid, DAGPlusPlusNodesDispatcher, NonStorePipelineErrorFactory, AvailablePluginsStore, myHelpers, HydratorPlusPlusCanvasFactory, HydratorPlusPlusConfigStore, HydratorPlusPlusPreviewActions, HydratorPlusPlusPreviewStore) {

    var vm = this;

    var dispatcher = DAGPlusPlusNodesDispatcher.getDispatcher();
    var undoListenerId = dispatcher.register('onUndoActions', resetEndpointsAndConnections);
    var redoListenerId = dispatcher.register('onRedoActions', resetEndpointsAndConnections);

    let localX, localY;

    const SHOW_METRICS_THRESHOLD = 0.8;

    const separation = $scope.separation || 200; // node separation length

    const nodeWidth = 200;
    const nodeHeight = 80;

    var dragged = false;

    vm.isDisabled = $scope.isDisabled;
    vm.disableNodeClick = $scope.disableNodeClick;

    var metricsPopovers = {};
    var selectedConnections = [];
    let conditionNodes = [];
    let normalNodes = [];
    let splitterNodesPorts = {};

    vm.pluginsMap = {};
    vm.adjacencyMap = DAGPlusPlusNodesStore.getAdjacencyMap();

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
    vm.nodeMenuOpen = null;

    vm.selectedNode = [];

    var commentsTimeout,
        nodesTimeout,
        fitToScreenTimeout,
        initTimeout,
        metricsPopoverTimeout,
        resetTimeout,
        highlightSelectedNodeConnectionsTimeout;

    var Mousetrap = window.CaskCommon.Mousetrap;

    vm.clearSelectedNodes = () => {
      vm.selectedNode = [];
      vm.instance.clearDragSelection();
      vm.instance.repaintEverything();
    };

    vm.selectNode = (event, node) => {
      if (vm.isDisabled) { return; }
      const isMultipleNodesDragged = document.querySelectorAll('.jsplumb-drag-selected');
      const isNodeAlreadyInSelection = vm.selectedNode.find(selectedNode => selectedNode.name === node.name);
      event.stopPropagation();

      /**
       * When users selects a bunch of nodes jsplumb adds jsplumb-drag-selected class to the nodes.
       *
       * After selecting the nodes, the user will click on one of the nodes and drag the selection around the canvas.
       * The click on the node shouldn't be considered as node selection. Hence we check if multiple nodes are being
       * dragged and if so just repaint instead of clearing out all selection and selecting that particular node.
       */
      if (isMultipleNodesDragged && isMultipleNodesDragged.length && isNodeAlreadyInSelection) {
        vm.instance.repaintEverything();
        return;
      }

      // If user clicks on a node with command/ctrl key pressed, keep adding the nodes the selection.
      if (vm.selectionBox.isMultiSelectEnabled) {
        vm.selectedNode.push(node);
        vm.highlightSelectedNodeConnections();
      } else {
        vm.selectedNode = [node];
        clearConnectionsSelection();
        vm.instance.clearDragSelection();
      }
      vm.instance.addToDragSelection(node.name);
      vm.instance.repaintEverything();
    };
    vm.getSelectedNodes = () => vm.selectedNode;
    /**
     * This is inconsistent when it comes to jsplumb. On connect or detach or click
     * we get the right connection object with proper source and target ids referring
     * to plugin nodes.
     * However when we query vm.instance.getConnnections({sourceId: ...})
     * It returns a connection object that is slightly different. The source
     * now points to the endpoint instead of the actual node. For the love of god
     * I don't know why but will need to file a issue and see what is going on. :sigh:
     */
    vm.getSelectedConnections = () => {
      const connectionsMap = {};
      $scope.connections.forEach(conn => {
        connectionsMap[`${conn.from}###${conn.to}`] = conn;
      });
      return selectedConnections
        .map(({source, target}) => {
          return {
            from: source.getAttribute('data-nodeid'),
            to: target.id
          };
        }).map(({from, to}) => {
          const originalConnection = connectionsMap[`${from}###${to}`];
          if (originalConnection) {
            return originalConnection;
          }
          return {from, to};
        });
    };
    vm.deleteSelectedNodes = () => vm.onKeyboardDelete();
    vm.onPluginContextMenuOpen = (nodeId) => {
      const isNodeAlreadySelected = vm.selectedNode.find(n => n.name === nodeId);
      if (isNodeAlreadySelected) {
        return;
      }
      const node = DAGPlusPlusNodesStore.getNodes().find(n => n.name === nodeId);
      if (!node) {
        return;
      }
      vm.selectedNode = [node];
      clearConnectionsSelection();
    };
    vm.isNodeSelected = (nodeName) => {
      if (!vm.selectedNode.length) {
        return false;
      }
      return vm.selectedNode.filter(node => node.name === nodeName).length > 0;
    };

    /**
     * Selection is for multi-select nodes/connections in the pipeline.
     *
     * toggle -
     *   This flag flips the mode between selection mode and move mode. We basically disable
     * dragging for the diagram-container and allow the <selection-box> to take
     * over the user selection
     *
     * isMultiSelectEnabled - 
     *   This flag is used when user clicks on command/ctrl and manually selects
     * individual nodes. This is a separate flag as when user selects a node we should
     * be able to differentiate between the normal selection (just clicking on a node)
     * vs command+click in which case the nodes selection behavior is slightly different.
     * The difference should be evident in the vm.selectNodes function
     *
     * isSelectionInProgress - 
     *   This flag is used to track if the user is currently selecting a bunch of nodes.
     * We need this flag to be able to easily go between selecting nodes and then clicking
     * on canvas to reset all the selection. This should be more evident in vm.handleCanvasClick
     * function.
     */
    vm.selectionBox = {
      boundaries: ['#diagram-container'],
      selectables: ['.box'],
      /**
       * It makes sense to have the events start -> move -> end to happen
       * in linear order. However under rare circumstances (cypress) this can
       * be out of order, meaning move gets fired before start event callback
       * is fired. The `isSelectionInProgress` is a catch all to make sure no
       * matter what the sequence of callback happens it is right.
       */
      isSelectionInProgress: false,
      toggle: vm.isDisabled ? false : true,
      isMultiSelectEnabled: false,
      start: () => {
        if (!vm.selectionBox.isSelectionInProgress) {
          vm.clearSelectedNodes();
          clearConnectionsSelection();
          vm.selectionBox.isSelectionInProgress = true;
        }
      },
      move: ({selected}) => {
        if (!vm.selectionBox.isSelectionInProgress) {
          vm.selectionBox.isSelectionInProgress = true;
        }
        const selectedNodes = $scope.nodes.filter(node => {
          if (selected.indexOf(node.name) !== -1) {
            return true;
          }
          return false;
        });

        /**
         * This has to be efficient for us to be able to handle large pipelines.
         * 
         * Current implementation:
         * 
         * I/P : nodes selected
         * 1. Get selected nodes from selection box.
         * 2. Get the adjacency map for the current graph
         * 3. Then iterate through selected nodes and for each node get all nodes connected to it from the adjacency map
         * 4. In the iteration if both the current selected node and the nodes connected to it are
         *    in the list of selected nodes then select the connection. This is where we use the
         *    selectedNodesMap to make a lookup.
         * 
         */
        vm.selectedNode = selectedNodes;
        vm.highlightSelectedNodeConnections();
      },
      end: () => {
        const nodesToAddToDrag = vm.selectedNode.map(node => node.name);
        vm.instance.addToDragSelection(nodesToAddToDrag);
      },
      toggleSelectionMode: () => {
        if (!vm.selectionBox.toggle) {
          vm.secondInstance.setDraggable('diagram-container', false);
          vm.selectionBox.toggle = true;
        } else {
          vm.secondInstance.setDraggable('diagram-container', true);
          vm.selectionBox.toggle = false;
        }
      }
    };
    const repaintTimeoutsMap = {};

    vm.pipelineArtifactType = HydratorPlusPlusConfigStore.getAppType();

    vm.highlightSelectedNodeConnections = () => {
      const selectedNodesMap = {};
      vm.selectedNode.forEach(node => selectedNodesMap[node.name] = true);
      const adjacencyMap = DAGPlusPlusNodesStore.getAdjacencyMap();
      clearConnectionsSelection();
      vm.selectedNode.forEach(({name}) => {
        const connectedNodes = adjacencyMap[name];
        if (!Array.isArray(connectedNodes)) {
          return;
        }
        const connectionsFromSource = vm.instance.getConnections({sourceId: name});
        connectedNodes.forEach(nodeId => {
          if (!selectedNodesMap[nodeId]) {
            return;
          }
          const connObj = connectionsFromSource.filter(conn => conn.source.getAttribute('data-nodeid') === name && conn.targetId === nodeId);
          if (connObj.length) {
            connObj.forEach(conn => {
              toggleConnection(conn, false);
            });
          }
        });
      });
    };
    vm.onPipelineContextMenuPaste = ({nodes, connections}) => {
      if (!Array.isArray(nodes) || !Array.isArray(connections)) {
        return;
      }
      vm.clearSelectedNodes();
      clearConnectionsSelection();
      let {nodes: newNodes, connections: newConnections} = sanitizeNodesAndConnectionsBeforePaste({nodes, connections});
      vm.selectedNode = newNodes;
      newNodes = [...$scope.nodes, ...newNodes];
      newConnections  = [...$scope.connections, ...newConnections];
      DAGPlusPlusNodesActionsFactory.createGraphFromConfigOnPaste(newNodes, newConnections, vm.comments);
      vm.instance.unbind('connection');
      vm.instance.unbind('connectionDetached');
      vm.instance.unbind('connectionMoved');
      vm.instance.unbind('beforeDrop');
      vm.instance.unbind('click');
      vm.instance.detachEveryConnection();
      init();
      $timeout.cancel(highlightSelectedNodeConnectionsTimeout);
      highlightSelectedNodeConnectionsTimeout = $timeout(() => vm.highlightSelectedNodeConnections());
      vm.instance.clearDragSelection();
    };
    vm.getPluginConfiguration = () => {
      if (!vm.selectedNode.length) {
        return;
      }
      return {
        stages: this.selectedNode.map((node) => {
          return {
            name: node.name,
            icon: node.icon,
            type: node.type,
            outputSchema: node.outputSchema,
            plugin: {
              name: node.plugin.name,
              artifact: node.plugin.artifact,
              properties: angular.copy(node.plugin.properties),
              label: node.plugin.label,
            },
          };
        })
      };
    };

    function repaintEverything() {
      const id = uuid.v4();

      repaintTimeoutsMap[id] =  $timeout(function () { vm.instance.repaintEverything(); })
        .then(() => {
          $timeout.cancel(repaintTimeoutsMap[id]);
          delete repaintTimeoutsMap[id];
        });
    }

    function init() {
      $scope.nodes = DAGPlusPlusNodesStore.getNodes();
      $scope.connections = DAGPlusPlusNodesStore.getConnections();
      vm.undoStates = DAGPlusPlusNodesStore.getUndoStates();
      vm.redoStates = DAGPlusPlusNodesStore.getRedoStates();
      vm.comments = DAGPlusPlusNodesStore.getComments();

      initTimeout = $timeout(function () {
        initNodes();
        addConnections();
        bindJsPlumbEvents();
        bindKeyboardEvents();

        if (vm.isDisabled) {
          disableAllEndpoints();
        }

        // Process metrics data
        if ($scope.showMetrics) {

          angular.forEach($scope.nodes, function (node) {
            var elem = angular.element(document.getElementById(node.name)).children();

            var scope = $rootScope.$new();
            scope.data = {
              node: node
            };
            scope.version = node.plugin.artifact.version;

            metricsPopovers[node.name] = {
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

          $scope.$watch('metricsData', function () {
            if (Object.keys($scope.metricsData).length === 0) {
              angular.forEach(metricsPopovers, function (value) {
                value.scope.data.metrics = 0;
              });
            }

            angular.forEach($scope.metricsData, function (pluginMetrics, pluginName) {
              let metricsToDisplay = {};
              let pluginMetricsKeys = Object.keys(pluginMetrics);
              for (let i = 0; i < pluginMetricsKeys.length; i++) {
                let pluginMetric = pluginMetricsKeys[i];
                if (typeof pluginMetrics[pluginMetric] === 'object') {
                  metricsToDisplay[pluginMetric] = _.sum(Object.keys(pluginMetrics[pluginMetric]).map(key => pluginMetrics[pluginMetric][key]));
                } else {
                  metricsToDisplay[pluginMetric] = pluginMetrics[pluginMetric];
                }
              }

              metricsPopovers[pluginName].scope.data.metrics = metricsToDisplay;
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
        vm.cleanUpGraph();
        vm.fitToScreen();
      }, 500);
    }

    function bindJsPlumbEvents() {
      vm.instance.bind('connection', addConnection);
      vm.instance.bind('connectionDetached', removeConnection);
      vm.instance.bind('connectionMoved', moveConnection);
      vm.instance.bind('beforeDrop', checkIfConnectionExistsOrValid);

      // jsPlumb docs say the event for clicking on an endpoint is called 'endpointClick',
      // but seems like the 'click' event is triggered both when clicking on an endpoint &&
      // clicking on a connection
      vm.instance.bind('click', toggleConnections);
    }

    function bindKeyboardEvents() {
      Mousetrap.bind(['command+z', 'ctrl+z'], vm.undoActions);
      Mousetrap.bind(['command+shift+z', 'ctrl+shift+z'], vm.redoActions);
      Mousetrap.bind(['del', 'backspace'], vm.onKeyboardDelete);
      Mousetrap.bind(['command+c', 'ctrl+c'], vm.onKeyboardCopy);

      if (vm.isDisabled) {
        return;
      }
      // Toggle between move mode. With spacebar users can move the entire canvas
      Mousetrap.bind('space', () => {
        $scope.$apply(function() {
          vm.secondInstance.setDraggable('diagram-container', false);
          vm.selectionBox.toggle = true;
        });
      }, 'keyup');
      Mousetrap.bind('space', () => {
        $scope.$apply(function() {
          vm.secondInstance.setDraggable('diagram-container', true);
          vm.selectionBox.toggle = false;
        });
      }, 'keydown');

      // Select all the nodes in the canvas.
      Mousetrap.bind('command+a', () => {
        const nodes = $scope.nodes;
        vm.selectedNode = nodes;
        vm.highlightSelectedNodeConnections();
        vm.instance.addToDragSelection(nodes.map(node => node.name));
        return false;
      });

      // Select multiple nodes by manually selecting nodes.
      Mousetrap.bind('command', () => {
        vm.selectionBox.isMultiSelectEnabled = true;
      }, 'keydown');
      Mousetrap.bind('command', () => {
        vm.selectionBox.isMultiSelectEnabled = false;
      }, 'keyup');
    }

    function unbindKeyboardEvents() {
      Mousetrap.unbind(['command+z', 'ctrl+z']);
      Mousetrap.unbind(['command+shift+z', 'ctrl+shift+z']);
      Mousetrap.unbind(['command+c', 'ctrl+c']);
      Mousetrap.unbind(['del', 'backspace']);
      Mousetrap.unbind('command');
      Mousetrap.unbind('space');
      Mousetrap.unbind('command+a');
    }

    function closeMetricsPopover(node) {
      var nodeInfo = metricsPopovers[node.name];
      if (metricsPopoverTimeout) {
        $timeout.cancel(metricsPopoverTimeout);
      }
      if (nodeInfo && nodeInfo.popover) {
        nodeInfo.popover.hide();
        nodeInfo.popover.destroy();
        nodeInfo.popover = null;
      }
    }

    vm.onKeyboardDelete = function onKeyboardDelete() {
      if (vm.selectedNode.length) {
        vm.onNodeDelete(null, vm.selectedNode);
      } else {
        vm.removeSelectedConnections();
      }
    };

    vm.nodeMouseEnter = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      var nodeInfo = metricsPopovers[node.name];

      if (metricsPopoverTimeout) {
        $timeout.cancel(metricsPopoverTimeout);
      }

      if (nodeInfo.element && nodeInfo.scope) {
        nodeInfo.popover = $modifiedPopover(nodeInfo.element, {
          trigger: 'manual',
          placement: 'auto right',
          target: angular.element(nodeInfo.element[0]),
          templateUrl: $scope.metricsPopoverTemplate,
          container: 'main',
          scope: nodeInfo.scope
        });
        nodeInfo.popover.$promise
          .then(function () {

            // Needs a timeout here to avoid showing popups instantly when just moving
            // cursor across a node
            metricsPopoverTimeout = $timeout(function () {
              if (nodeInfo.popover && typeof nodeInfo.popover.show === 'function') {
                nodeInfo.popover.show();
              }
            }, 500);
          });
      }
    };

    vm.nodeMouseLeave = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      closeMetricsPopover(node);
    };

    vm.zoomIn = function () {
      vm.scale += 0.1;

      setZoom(vm.scale, vm.instance);
    };

    vm.zoomOut = function () {
      if (vm.scale <= 0.2) { return; }

      vm.scale -= 0.1;
      setZoom(vm.scale, vm.instance);
    };

    /**
     * Utily function from jsPlumb
     * https://jsplumbtoolkit.com/community/doc/zooming.html
     *
     * slightly modified to fit our needs
     **/
    function setZoom(zoom, instance, transformOrigin, el) {
      if ($scope.nodes.length === 0) { return; }

      transformOrigin = transformOrigin || [0.5, 0.5];
      instance = instance || jsPlumb;
      el = el || instance.getContainer();
      var p = ['webkit', 'moz', 'ms', 'o'],
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

    function initNodes() {
      angular.forEach($scope.nodes, function (node) {
        if (node.type === 'condition') {
          initConditionNode(node.name);
        } else if (node.type === 'splittertransform') {
          initSplitterNode(node);
        } else {
          initNormalNode(node);
        }

        if (!vm.instance.isTarget(node.name)) {
          let targetOptions = Object.assign({}, vm.targetNodeOptions);
          if (node.type === 'alertpublisher') {
            targetOptions.scope = 'alertScope';
          } else if (node.type === 'errortransform') {
            targetOptions.scope = 'errorScope';
          }

          // Disabling the ability to disconnect a connection from target
          if (vm.isDisabled) {
            targetOptions.connectionsDetachable = false;
          }
          vm.instance.makeTarget(node.name, targetOptions);
        }
      });
    }

    function initNormalNode(node) {
      if (normalNodes.indexOf(node.name) !== -1) {
        return;
      }
      addEndpointForNormalNode('endpoint_' + node.name);
      if (!_.isEmpty(vm.pluginsMap) && !vm.isDisabled) {
        addErrorAlertEndpoints(node);
      }
      normalNodes.push(node.name);
    }

    function initConditionNode(nodeName) {
      if (conditionNodes.indexOf(nodeName) !== -1) {
        return;
      }
      addEndpointForConditionNode('endpoint_' + nodeName + '_condition_true', vm.conditionTrueEndpointStyle, 'yesLabel');
      addEndpointForConditionNode('endpoint_' + nodeName + '_condition_false', vm.conditionFalseEndpointStyle, 'noLabel');
      conditionNodes.push(nodeName);
    }

    function initSplitterNode(node) {
      if (!node.outputSchema || !Array.isArray(node.outputSchema) || (Array.isArray(node.outputSchema) && node.outputSchema[0].name === GLOBALS.defaultSchemaName)) {
        let splitterPorts = splitterNodesPorts[node.name];
        if (!_.isEmpty(splitterPorts)) {
          angular.forEach(splitterPorts, (port) => {
            let portElId = 'endpoint_' + node.name + '_port_' + port;
            deleteEndpoints(portElId);
          });
          DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
          delete splitterNodesPorts[node.name];
        }
        return;
      }

      let newPorts = node.outputSchema
        .map(schema => schema.name);

      let splitterPorts = splitterNodesPorts[node.name];

      let portsChanged = !_.isEqual(splitterPorts, newPorts);

      if (!portsChanged) {
        return;
      }

      angular.forEach(splitterPorts, (port) => {
        let portElId = 'endpoint_' + node.name + '_port_' + port;
        deleteEndpoints(portElId);
      });

      angular.forEach(node.outputSchema, (outputSchema) => {
        addEndpointForSplitterNode('endpoint_' + node.name + '_port_' + outputSchema.name);
      });

      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
      splitterNodesPorts[node.name] = newPorts;
    }

    function addEndpointForNormalNode(endpointDOMId, customConfig) {
      let endpointDOMEl = document.getElementById(endpointDOMId);
      let endpointObj = Object.assign({}, {
        isSource: true,
        cssClass: `plugin-${endpointDOMId}-right`
      }, customConfig);
      if (vm.isDisabled) {
        endpointObj.enabled = false;
      }
      let endpoint = vm.instance.addEndpoint(endpointDOMEl, endpointObj);
      addListenersForEndpoint(endpoint, endpointDOMEl);
    }

    function addEndpointForConditionNode(endpointDOMId, endpointStyle, overlayLabel) {
      let endpointDOMEl = document.getElementById(endpointDOMId);
      endpointStyle.cssClass += ` plugin-${endpointDOMId}`;
      let newEndpoint = vm.instance.addEndpoint(endpointDOMEl, endpointStyle);
      newEndpoint.hideOverlay(overlayLabel);
      addListenersForEndpoint(newEndpoint, endpointDOMEl, overlayLabel);
    }

    function addEndpointForSplitterNode(endpointDOMId) {
      let endpointDOMEl = document.getElementsByClassName(endpointDOMId);
      endpointDOMEl = endpointDOMEl[endpointDOMEl.length - 1];

      let splitterEndpointStyleWithUUID = Object.assign({}, vm.splitterEndpointStyle, { uuid: endpointDOMId });
      splitterEndpointStyleWithUUID.cssClass += `plugin-${endpointDOMId}`;
      let splitterEndpoint = vm.instance.addEndpoint(endpointDOMEl, splitterEndpointStyleWithUUID);
      addListenersForEndpoint(splitterEndpoint, endpointDOMEl);
    }

    function addConnections() {
      angular.forEach($scope.connections, function (conn) {
        var sourceNode = $scope.nodes.find(node => node.name === conn.from);
        var targetNode = $scope.nodes.find(node => node.name === conn.to);

        if (!sourceNode || !targetNode) {
          return;
        }

        let connObj = {
          target: conn.to
        };

        if (conn.hasOwnProperty('condition')) {
          connObj.source = vm.instance.getEndpoints(`endpoint_${conn.from}_condition_${conn.condition}`)[0];
        } else if (conn.hasOwnProperty('port')) {
          connObj.source = vm.instance.getEndpoint(`endpoint_${conn.from}_port_${conn.port}`);
        } else if (targetNode.type === 'errortransform' || targetNode.type === 'alertpublisher') {
          if (!_.isEmpty(vm.pluginsMap) && !vm.isDisabled) {
            addConnectionToErrorsAlerts(conn, sourceNode, targetNode);
            return;
          }
        } else {
          connObj.source = vm.instance.getEndpoints(`endpoint_${conn.from}`)[0];
        }

        if (connObj.source && connObj.target) {
          let newConn = vm.instance.connect(connObj);
          if (
            targetNode.type === 'condition' ||
            sourceNode.type === 'action' ||
            targetNode.type === 'action' ||
            sourceNode.type === 'sparkprogram' ||
            targetNode.type === 'sparkprogram'
          ) {
            newConn.setType('dashed');
          }
        }
      });
    }

    function addErrorAlertEndpoints(node) {
      if (vm.shouldShowAlertsPort(node)) {
        addEndpointForNormalNode('endpoint_' + node.name + '_alert', vm.alertEndpointStyle);
      }
      if (vm.shouldShowErrorsPort(node)) {
        addEndpointForNormalNode('endpoint_' + node.name + '_error', vm.errorEndpointStyle);
      }
    }

    const addConnectionToErrorsAlerts = (conn, sourceNode, targetNode) => {
      let connObj = {
        target: conn.to
      };

      if (targetNode.type === 'errortransform' && vm.shouldShowErrorsPort(sourceNode)) {
        connObj.source = vm.instance.getEndpoints(`endpoint_${conn.from}_error`)[0];
      } else if (targetNode.type === 'alertpublisher' && vm.shouldShowAlertsPort(sourceNode)) {
        connObj.source = vm.instance.getEndpoints(`endpoint_${conn.from}_alert`)[0];
      } else {
        connObj.source = vm.instance.getEndpoints(`endpoint_${conn.from}`)[0];
        // this is for backwards compability with old pipelines where we don't specify
        // emit-alerts and emit-error in the plugin config yet. In those cases we should
        // still connect to the Error Collector/Alert Publisher using the normal endpoint
        let scopeString = vm.instance.getDefaultScope() + ' alertScope errorScope';
        connObj.source.scope = scopeString;
      }
      let defaultConnectorSettings = vm.defaultDagSettings.Connector;
      connObj.connector = [defaultConnectorSettings[0], Object.assign({}, defaultConnectorSettings[1], { midpoint: 0 })];

      vm.instance.connect(connObj);
    };

    function addErrorAlertsEndpointsAndConnections() {
      // Need the timeout because it takes an Angular tick for the Alert and Error port DOM elements
      // to show up after vm.pluginsMap is populated
      let addErrorAlertEndpointsTimeout = $timeout(() => {
        angular.forEach($scope.nodes, (node) => {
          addErrorAlertEndpoints(node);
        });
        vm.instance.unbind('connection');
        angular.forEach($scope.connections, (conn) => {
          var sourceNode = $scope.nodes.find(node => node.name === conn.from);
          var targetNode = $scope.nodes.find(node => node.name === conn.to);

          if (!sourceNode || !targetNode) {
            return;
          }

          if (targetNode.type === 'errortransform' || targetNode.type === 'alertpublisher') {
            addConnectionToErrorsAlerts(conn, sourceNode, targetNode);
          }
        });
        vm.instance.bind('connection', addConnection);
        repaintEverything();
        $timeout.cancel(addErrorAlertEndpointsTimeout);
      });
    }

    function transformCanvas (top, left) {
      const newTop = top + vm.panning.top;
      const newLeft = left + vm.panning.left;

      vm.setCanvasPanning(newTop, newLeft);
    }

    vm.setCanvasPanning = (top, left) => {
      vm.panning.top = top;
      vm.panning.left = left;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };
    };

    vm.handleCanvasClick = () => {
      if(vm.selectionBox.isSelectionInProgress) {
        vm.selectionBox.isSelectionInProgress = false;
        return;
      }
      vm.instance.clearDragSelection();
      vm.toggleNodeMenu();
      clearConnectionsSelection();
      vm.clearSelectedNodes();
      vm.clearCommentSelection();
    };

    function addConnection(newConnObj) {
      let connection = {
        from: newConnObj.sourceId,
        to: newConnObj.targetId
      };

      const source = newConnObj.source.getAttribute('data-nodetype');
      const fromNodeId = newConnObj.source.getAttribute('data-nodeid');
      connection.from = fromNodeId;

      if (source === 'splitter') {
        connection.port = newConnObj.source.getAttribute('data-portname');
      } else if (source.indexOf('condition') !== -1) {
        connection.condition = source === 'condition-true' ? 'true' : 'false';
      }
      $scope.connections.push(connection);
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    }

    function removeConnection(detachedConnObj, updateStore = true) {
      let connObj = Object.assign({}, detachedConnObj);
      if (!detachedConnObj.source || typeof detachedConnObj.source !== 'object') {
        return;
      }
      const nodeId = detachedConnObj.source.getAttribute('data-nodeid');
      connObj.sourceId = nodeId;
      var connectionIndex = _.findIndex($scope.connections, function (conn) {
        return conn.from === connObj.sourceId && conn.to === connObj.targetId;
      });
      if (connectionIndex !== -1) {
        $scope.connections.splice(connectionIndex, 1);
      }
      if (updateStore) {
        DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
      }
    }

    function moveConnection(moveInfo) {
      let oldConnection = {
        sourceId: moveInfo.originalSourceId,
        targetId: moveInfo.originalTargetId
      };
      if (myHelpers.objectQuery(moveInfo, 'originalSourceEndpoint', 'element')) {
        oldConnection.source = moveInfo.originalSourceEndpoint.element;
      }
      if (myHelpers.objectQuery(moveInfo, 'originalTargetEndpoint', 'element')) {
        oldConnection.target = moveInfo.originalTargetEndpoint.element;
      }
      // don't need to call addConnection for the new connection, since that will be done
      // automatically as part of the 'connection' event
      removeConnection(oldConnection, false);
    }

    vm.removeSelectedConnections = function() {
      if (selectedConnections.length === 0 || vm.isDisabled) { return; }

      vm.instance.unbind('connectionDetached');
      angular.forEach(selectedConnections, function (selectedConnectionObj) {
        removeConnection(selectedConnectionObj, false);
        vm.instance.detach(selectedConnectionObj);
      });
      vm.instance.bind('connectionDetached', removeConnection);
      selectedConnections = [];
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    };

    function toggleConnections(selectedObj, event) {
      if (vm.isDisabled) { return; }

      vm.clearSelectedNodes();

      // is connection
      if (selectedObj.sourceId && selectedObj.targetId) {
        toggleConnection(selectedObj);
        return;
      }

      if (!selectedObj.connections || !selectedObj.connections.length) {
        return;
      }

      // else is endpoint
      if (selectedObj.isTarget) {
        toggleConnection(selectedObj.connections[0]);
        return;
      }

      let connectionsToToggle = selectedObj.connections;

      let notYetSelectedConnections = _.difference(connectionsToToggle, selectedConnections);

      // This is to toggle all connections coming from an endpoint.
      // If zero, one or more (but not all) of the connections are already selected,
      // then just select the remaining ones. Else if they're all selected,
      // then unselect them.

      if (notYetSelectedConnections.length !== 0) {
        notYetSelectedConnections.forEach(connection => {
          selectedConnections.push(connection);
          connection.addClass('selected-connector');
          connection.addType('selected');
        });
      } else {
        connectionsToToggle.forEach(connection => {
          selectedConnections.splice(selectedConnections.indexOf(connection), 1);
          connection.removeClass('selected-connector');
          connection.removeType('selected');
        });
      }
      if (event) {
        event.stopPropagation();
        event.stopImmediatePropagation();
        event.preventDefault();
      }
    }

    function toggleConnection(connObj, toggle = true) {
      if (!connObj) {
        return;
      }

      if (selectedConnections.indexOf(connObj) === -1) {
        selectedConnections.push(connObj);
      } else {
        selectedConnections.splice(selectedConnections.indexOf(connObj), 1);
      }
      if (!toggle) {
        connObj.addClass('selected-connector');
        connObj.addType('selected');
        return;
      }
      connObj.toggleType('selected');
      connObj.removeClass('selected-connector');
    }

    function clearConnectionsSelection() {
      selectedConnections.forEach((conn) => {
        const existingTypes = conn.getType();
        if (Array.isArray(existingTypes) && existingTypes.indexOf('selected') !== -1) {
          conn.toggleType('selected');
          conn.removeClass('selected-connector');
        }
      });

      selectedConnections = [];
    }

    function deleteEndpoints(elementId) {
      vm.instance.unbind('connectionDetached');
      let endpoint = vm.instance.getEndpoint(elementId);

      if (endpoint && endpoint.connections) {
        angular.forEach(endpoint.connections, (conn) => {
          removeConnection(conn, false);
          vm.instance.detach(conn);
        });
      }

      vm.instance.deleteEndpoint(endpoint);
      vm.instance.bind('connectionDetached', removeConnection);
    }

    function disableEndpoint(uuid) {
      let endpoint = vm.instance.getEndpoint(uuid);
      if (endpoint) {
        endpoint.setEnabled(false);
      }
    }

    function disableEndpoints(elementId) {
      let endpointArr = vm.instance.getEndpoints(elementId);

      if (endpointArr) {
        angular.forEach(endpointArr, (endpoint) => {
          endpoint.setEnabled(false);
        });
      }
    }

    function disableAllEndpoints() {
      angular.forEach($scope.nodes, function (node) {
        if (node.plugin.type === 'condition') {
          let endpoints = [`endpoint_${node.name}_condition_true`, `endpoint_${node.name}_condition_false`];
          angular.forEach(endpoints, (endpoint) => {
            disableEndpoints(endpoint);
          });
        } else if (node.plugin.type === 'splittertransform')  {
          let portNames = node.outputSchema.map(port => port.name);
          let endpoints = portNames.map(portName => `endpoint_${node.name}_port_${portName}`);
          angular.forEach(endpoints, (endpoint) => {
            // different from others because the name here is the uuid of the splitter endpoint,
            // not the id of DOM element
            disableEndpoint(endpoint);
          });
        } else {
          disableEndpoints('endpoint_' + node.name);
          if (vm.shouldShowAlertsPort(node)) {
            disableEndpoints('endpoint_' + node.name + '_alert');
          }
          if (vm.shouldShowErrorsPort(node)) {
            disableEndpoints('endpoint_' + node.name + '_error');
          }
        }
      });
    }

    function addHoverListener(endpoint, domCircleEl, labelId) {
      if (!domCircleEl.classList.contains('hover')) {
        domCircleEl.classList.add('hover');
      }
      if (labelId) {
        endpoint.showOverlay(labelId);
      }
    }

    function removeHoverListener(endpoint, domCircleEl, labelId) {
      if (domCircleEl.classList.contains('hover')) {
        domCircleEl.classList.remove('hover');
      }
      if (labelId) {
        endpoint.hideOverlay(labelId);
      }
    }

    function addListenersForEndpoint(endpoint, domCircleEl, labelId) {
      endpoint.canvas.removeEventListener('mouseover', addHoverListener);
      endpoint.canvas.removeEventListener('mouseout', removeHoverListener);
      endpoint.canvas.addEventListener('mouseover', addHoverListener.bind(null, endpoint, domCircleEl, labelId));
      endpoint.canvas.addEventListener('mouseout', removeHoverListener.bind(null, endpoint, domCircleEl, labelId));
    }

    function checkIfConnectionExistsOrValid(connObj) {
      // return false if connection already exists, which will prevent the connecton from being formed

      connObj.sourceId = connObj.connection.source.getAttribute('data-nodeid');
      var exists = _.find($scope.connections, function (conn) {
        return conn.from === connObj.sourceId && conn.to === connObj.targetId;
      });

      var sameNode = connObj.sourceId === connObj.targetId;

      if (exists || sameNode) {
        return false;
      }

      // else check if the connection is valid
      var sourceNode = $scope.nodes.find(node => node.name === connObj.sourceId);
      var targetNode = $scope.nodes.find( node => node.name === connObj.targetId);

      var valid = true;

      NonStorePipelineErrorFactory.connectionIsValid(sourceNode, targetNode, function(invalidConnection) {
        if (invalidConnection) { valid = false; }
      });

      if (!valid) {
        return valid;
      }

      // If valid, then modifies the look of the connection before showing it
      if (
        sourceNode.type === 'action' ||
        targetNode.type === 'action' ||
        sourceNode.type === 'sparkprogram' ||
        targetNode.type === 'sparkprogram'
      ) {
        connObj.connection.setType('dashed');
      } else if (sourceNode.type !== 'condition' && targetNode.type !== 'condition') {
        connObj.connection.setType('basic solid');
      } else {
        if (sourceNode.type === 'condition') {
          if (connObj.connection.endpoints && connObj.connection.endpoints.length > 0) {
            let sourceEndpoint = connObj.dropEndpoint;
            const nodeType = sourceEndpoint.canvas.getAttribute('data-nodetype');
            if (nodeType === 'condition-true') {
              connObj.connection.setType('conditionTrue');
            } if (nodeType === 'condition-false') {
              connObj.connection.setType('conditionFalse');
            }
          }
        } else {
          connObj.connection.setType('basic');
        }
        if (targetNode.type === 'condition') {
          connObj.connection.addType('dashed');
        }
      }

      repaintEverything();
      return valid;
    }

    function resetEndpointsAndConnections() {
      if (resetTimeout) {
        $timeout.cancel(resetTimeout);
      }

      resetTimeout = $timeout(function () {
        vm.instance.reset();
        normalNodes = [];
        conditionNodes = [];
        splitterNodesPorts = {};

        $scope.nodes = DAGPlusPlusNodesStore.getNodes();
        $scope.connections = DAGPlusPlusNodesStore.getConnections();
        vm.undoStates = DAGPlusPlusNodesStore.getUndoStates();
        vm.redoStates = DAGPlusPlusNodesStore.getRedoStates();
        makeNodesDraggable();
        initNodes();
        addConnections();
        selectedConnections = [];
        bindJsPlumbEvents();

        if (commentsTimeout) {
          vm.comments = DAGPlusPlusNodesStore.getComments();
          $timeout.cancel(commentsTimeout);
        }

        commentsTimeout = $timeout(function () {
          makeCommentsDraggable();
        });
      });
    }

    function makeNodesDraggable() {
      if (vm.isDisabled) { return; }

      var nodes = document.querySelectorAll('.box');

      vm.instance.draggable(nodes, {
        start: function (drag) {
          let currentCoordinates = {
            x: drag.e.clientX,
            y: drag.e.clientY,
          };
          if (currentCoordinates.x === localX && currentCoordinates.y === localY) {
            return;
          }
          localX = currentCoordinates.x;
          localY = currentCoordinates.y;

          dragged = true;
          const nodeName = drag.el.getAttribute('id');
          const isNodeAlreadySelected = vm.selectedNode.find(selectedNode => selectedNode.name === nodeName);
          if (!isNodeAlreadySelected) {
            vm.instance.clearDragSelection();
          }
        },
        stop: function (dragEndEvent) {
          var config = {
            _uiPosition: {
              top: dragEndEvent.el.style.top,
              left: dragEndEvent.el.style.left
            }
          };
          DAGPlusPlusNodesActionsFactory.updateNode(dragEndEvent.el.id, config);
        }
      });
    }

    function makeCommentsDraggable() {
      var comments = document.querySelectorAll('.comment-box');
      vm.instance.draggable(comments, {
        start: function () {
          vm.clearSelectedNodes();
          clearConnectionsSelection();
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
    }

    vm.selectEndpoint = function(event, node) {
      if (event.target.className.indexOf('endpoint-circle') === -1) { return; }
      vm.clearSelectedNodes();

      let sourceElem = node.name;
      let endpoints = vm.instance.getEndpoints(sourceElem);

      if (!endpoints) { return; }

      for (let i = 0; i < endpoints.length; i++) {
        let endpoint = endpoints[i];
        if (endpoint.connections && endpoint.connections.length > 0) {
          if (endpoint.connections[0].sourceId === node.name) {
            toggleConnections(endpoint);
            break;
          }
        }
      }
      event.stopPropagation();
      event.preventDefault();
      event.stopImmediatePropagation();
    };

    jsPlumb.ready(function() {
      var dagSettings = DAGPlusPlusFactory.getSettings();
      var {defaultDagSettings, defaultConnectionStyle, selectedConnectionStyle, dashedConnectionStyle, solidConnectionStyle, conditionTrueConnectionStyle, conditionTrueEndpointStyle, conditionFalseConnectionStyle, conditionFalseEndpointStyle, splitterEndpointStyle, alertEndpointStyle, errorEndpointStyle, targetNodeOptions} = dagSettings;
      vm.defaultDagSettings = defaultDagSettings;
      vm.conditionTrueEndpointStyle = conditionTrueEndpointStyle;
      vm.conditionFalseEndpointStyle = conditionFalseEndpointStyle;
      vm.splitterEndpointStyle = splitterEndpointStyle;
      vm.alertEndpointStyle = alertEndpointStyle;
      vm.errorEndpointStyle = errorEndpointStyle;
      vm.targetNodeOptions = targetNodeOptions;

      vm.instance = jsPlumb.getInstance(defaultDagSettings);
      vm.instance.registerConnectionType('basic', defaultConnectionStyle);
      vm.instance.registerConnectionType('selected', selectedConnectionStyle);
      vm.instance.registerConnectionType('dashed', dashedConnectionStyle);
      vm.instance.registerConnectionType('solid', solidConnectionStyle);
      vm.instance.registerConnectionType('conditionTrue', conditionTrueConnectionStyle);
      vm.instance.registerConnectionType('conditionFalse', conditionFalseConnectionStyle);

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
          }
        });
        if (!vm.isDisabled) {
          vm.secondInstance.setDraggable('diagram-container', false);
        }
      }

      // doing this to listen to changes to just $scope.nodes instead of everything else
      $scope.$watch('nodes', function() {
        if (!vm.isDisabled) {
          if (nodesTimeout) {
            $timeout.cancel(nodesTimeout);
          }
          nodesTimeout = $timeout(function () {
            makeNodesDraggable();
            initNodes();
            makeCommentsDraggable();
            /**
             * TODO(https://issues.cask.co/browse/CDAP-16423): Need to debug why setting zoom on init doesn't set the correct zoom
             *
             * Without this, on initial load the nodes drag is weird. The cursor travels outside the node
             * meaning the nodes are dragged only to some extent and not along with the mouse cursor.
             * The underlying reason is that the zoom is incorrect in the graph. Once the zoom is set
             * right the drag happens correctly.
             *
             * This is a escape hatch for us to set zoom and make dragging
             * right one each node addition. This is not a perfect solution
             */
            setZoom(vm.instance.getZoom(), vm.instance);
          });
        }
      }, true);

      // This is needed to redraw connections and endpoints on browser resize
      angular.element($window).on('resize', vm.instance.repaintEverything);

      DAGPlusPlusNodesStore.registerOnChangeListener(function () {
        vm.activeNodeId = DAGPlusPlusNodesStore.getActiveNodeId();

        // can do keybindings only if no node is selected
        if (!vm.activeNodeId) {
          bindKeyboardEvents();
        } else {
          unbindKeyboardEvents();
        }
      });
    });

    vm.onPreviewData = function(event, node) {
      event.stopPropagation();
      HydratorPlusPlusPreviewStore.dispatch(HydratorPlusPlusPreviewActions.setPreviewData());
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onNodeClick = function(event, node) {
      closeMetricsPopover(node);

      window.CaskCommon.PipelineMetricsActionCreator.setMetricsTabActive(false);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onMetricsClick = function(event, node, portName) {
      event.stopPropagation();
      if ($scope.disableMetricsClick) {
        return;
      }
      closeMetricsPopover(node);
      window.CaskCommon.PipelineMetricsActionCreator.setMetricsTabActive(true, portName);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onNodeDelete = function (event, nodes = vm.selectedNode) {
      if (event) {
        event.stopPropagation();
      }

      const newNodes = angular.copy(nodes);
      newNodes.forEach(node => {
        DAGPlusPlusNodesActionsFactory.removeNode(node.name);

        if (Object.keys(splitterNodesPorts).indexOf(node.name) !== -1) {
          delete splitterNodesPorts[node.name];
        }
        let nodeType = node.plugin.type || node.type;
        if (nodeType === 'splittertransform' && node.outputSchema && Array.isArray(node.outputSchema)) {
          let portNames = node.outputSchema.map(port => port.name);
          let endpoints = portNames.map(portName => `endpoint_${node.name}_port_${portName}`);
          angular.forEach(endpoints, (endpoint) => {
            deleteEndpoints(endpoint);
          });
        }

        vm.instance.unbind('connectionDetached');
        selectedConnections = selectedConnections.filter(function(selectedConnObj) {
          return selectedConnObj.sourceId !== node.name && selectedConnObj.targetId !== node.name;
        });
        vm.instance.unmakeTarget(node.name);
        vm.instance.remove(node.name);
      });
      vm.instance.bind('connectionDetached', removeConnection);

      vm.clearSelectedNodes();
    };

    vm.cleanUpGraph = function () {
      if ($scope.nodes.length === 0) { return; }

      let newConnections = HydratorPlusPlusCanvasFactory.orderConnections($scope.connections, HydratorPlusPlusConfigStore.getAppType() || window.CaskCommon.PipelineDetailStore.getState().artifact.name, $scope.nodes);
      let connectionsSwapped = false;
      for (let i = 0; i < newConnections.length; i++) {
        if (newConnections[i].from !== $scope.connections[i].from || newConnections[i].to !== $scope.connections[i].to) {
          connectionsSwapped = true;
          break;
        }
      }

      if (connectionsSwapped) {
        $scope.connections = newConnections;
        DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
      }

      let graphNodesNetworkSimplex = DAGPlusPlusFactory.getGraphLayout($scope.nodes, $scope.connections, separation)._nodes;
      let graphNodesLongestPath = DAGPlusPlusFactory.getGraphLayout($scope.nodes, $scope.connections, separation, 'longest-path')._nodes;

      angular.forEach($scope.nodes, function (node) {
        let locationX = graphNodesNetworkSimplex[node.name].x;
        let locationY = graphNodesLongestPath[node.name].y;
        node._uiPosition = {
          left: locationX - 50 + 'px',
          top: locationY + 'px'
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

    vm.toggleNodeMenu = function (node, event) {
      if (event) {
        event.preventDefault();
        event.stopPropagation();
      }

      if (!node || vm.nodeMenuOpen === node.name) {
        vm.nodeMenuOpen = null;
      } else {
        vm.nodeMenuOpen = node.name;
        vm.selectedNode = [node];
      }
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
        if (node._uiPosition.left.indexOf('vw') !== -1) {
          var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }
        return parseInt(node._uiPosition.left, 10);
      });
      var maxLeft = _.max($scope.nodes, function (node) {
        if (node._uiPosition.left.indexOf('vw') !== -1) {
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
       * between the furthest nodes
       **/
      var width = parseInt(maxLeft._uiPosition.left, 10) - parseInt(minLeft._uiPosition.left, 10) + nodeWidth;
      var height = parseInt(maxTop._uiPosition.top, 10) - parseInt(minTop._uiPosition.top, 10) + nodeHeight;

      var parent = $scope.element[0].parentElement.getBoundingClientRect();

      // margins from the furthest nodes to the edge of the canvas (75px each)
      var leftRightMargins = 150;
      var topBottomMargins = 150;

      // calculating the scales and finding the minimum scale
      var widthScale = (parent.width - leftRightMargins) / width;
      var heightScale = (parent.height - topBottomMargins) / height;

      vm.scale = Math.min(widthScale, heightScale);

      if (vm.scale > 1) {
        vm.scale = 1;
      }
      setZoom(vm.scale, vm.instance);


      // This will move all nodes by the minimum left and minimum top
      var offsetLeft = parseInt(minLeft._uiPosition.left, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.left = (parseInt(node._uiPosition.left, 10) - offsetLeft) + 'px';
      });

      var offsetTop = parseInt(minTop._uiPosition.top, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.top = (parseInt(node._uiPosition.top, 10) - offsetTop) + 'px';
      });

      $scope.getGraphMargins($scope.nodes);

      vm.panning.left = 0;
      vm.panning.top = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      DAGPlusPlusNodesActionsFactory.resetPluginCount();
      DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);

      repaintEverything();
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
      if (commentsTimeout) {
        vm.comments = DAGPlusPlusNodesStore.getComments();
        $timeout.cancel(commentsTimeout);
      }
      commentsTimeout = $timeout(function () {
        makeCommentsDraggable();
      });
    };

    vm.clearCommentSelection = function clearCommentSelection() {
      angular.forEach(vm.comments, function (comment) {
        comment.isActive = false;
      });
    };

    vm.commentSelect = function (event, comment) {
      event.stopPropagation();
      vm.clearCommentSelection();

      if (dragged) {
        dragged = false;
        return;
      }

      comment.isActive = true;
    };

    vm.deleteComment = function (comment) {
      DAGPlusPlusNodesActionsFactory.deleteComment(comment);
    };

    vm.undoActions = function () {
      if (!vm.isDisabled && vm.undoStates.length > 0) {
        DAGPlusPlusNodesActionsFactory.undoActions();
      }
    };

    vm.redoActions = function () {
      if (!vm.isDisabled && vm.redoStates.length > 0) {
        DAGPlusPlusNodesActionsFactory.redoActions();
      }
    };

    vm.shouldShowAlertsPort = (node) => {
      let key = generatePluginMapKey(node);

      return myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'emit-alerts');
    };

    vm.shouldShowErrorsPort = (node) => {
      let key = generatePluginMapKey(node);

      return myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'emit-errors');
    };

    vm.onKeyboardCopy = function onKeyboardCopy() {
      const stages = vm.getPluginConfiguration().stages;
      const connections =  vm.getSelectedConnections();
      vm.nodeMenuOpen = null;
      window.CaskCommon.Clipboard.copyToClipBoard(JSON.stringify({
        stages,
        connections
      }));
    };

    // handling node paste
    document.body.onpaste = (e) => {
      const activeNode = DAGPlusPlusNodesStore.getActiveNodeId();
      const target = myHelpers.objectQuery(e, 'target', 'tagName');
      const INVALID_TAG_NAME = ['INPUT', 'TEXTAREA'];

      if (activeNode || INVALID_TAG_NAME.indexOf(target) !== -1) {
        return;
      }

      let config;
      if (window.clipboardData && window.clipboardData.getData) {
        // for IE......
        config = window.clipboardData.getData('Text');
      } else {
        config = e.clipboardData.getData('text/plain');
      }
      try {
        config = JSON.parse(config);
      } catch(err) {
        console.error('Unable to paste to canvas: '+ err);
      }
      config.nodes = config.stages;
      delete config.stages;
      vm.onPipelineContextMenuPaste(config);
    };

    function sanitizeNodesAndConnectionsBeforePaste(text) {
      try {
        let config = {};
        if (typeof text === 'string') {
          config = JSON.parse(text);
        } else {
          config = text;
        }
        let nodes = myHelpers.objectQuery(config, 'nodes');
        let connections = myHelpers.objectQuery(config, 'connections');
        const oldNameToNewNameMap = {};
        if (!nodes || !Array.isArray(nodes)) {
          return;
        }

        nodes = nodes.map(node => {
          if (!node) { return; }

          // change name
          let newName = `${node.plugin.label.replace(/[ \/]/g, '-')}`;
          const filteredNodes = HydratorPlusPlusConfigStore.getNodes()
            .filter(filteredNode => {
              return filteredNode.plugin.label ? filteredNode.plugin.label.startsWith(newName) : false;
            });
          const randIndex = Math.floor(Math.random() * 100);
          newName = filteredNodes.length > 0 ? `${newName}${randIndex}` : newName;
          let iconConfiguration = {};
          if (!node.icon) {
            iconConfiguration = Object.assign({}, {
              icon: DAGPlusPlusFactory.getIcon(node.plugin.name)
            });
          }

          oldNameToNewNameMap[node.name] = newName;
          node.plugin.label = newName;
          return Object.assign({}, node, {
            name: oldNameToNewNameMap[node.name]
          }, iconConfiguration);
        });
        connections = connections.map((connection) => {
          const from = connection.from;
          const to = connection.to;
          return Object.assign({}, connection, {
            from: oldNameToNewNameMap[from] || from,
            to: oldNameToNewNameMap[to] || to,
          });
        });
        return {nodes, connections};
      } catch (e) {
        console.log('error parsing node config', e);
      }
    }

    // CUSTOM ICONS CONTROL
    function generatePluginMapKey(node) {
      let plugin = node.plugin;
      let type = node.type || plugin.type;

      return `${plugin.name}-${type}-${plugin.artifact.name}-${plugin.artifact.version}-${plugin.artifact.scope}`;
    }

    vm.shouldShowCustomIcon = (node) => {
      let key = generatePluginMapKey(node);

      let iconSourceType = myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'icon', 'type');
      return ['inline', 'link'].indexOf(iconSourceType) !== -1;
    };

    vm.getCustomIconSrc = (node) => {
      let key = generatePluginMapKey(node);
      let iconSourceType = myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'icon', 'type');

      if (iconSourceType === 'inline') {
        return myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'icon', 'arguments', 'data');
      }

      return myHelpers.objectQuery(vm.pluginsMap, key, 'widgets', 'icon', 'arguments', 'url');
    };

    let subAvailablePlugins = AvailablePluginsStore.subscribe(() => {
      vm.pluginsMap = AvailablePluginsStore.getState().plugins.pluginsMap;
      if (!_.isEmpty(vm.pluginsMap)) {
        addErrorAlertsEndpointsAndConnections();
        subAvailablePlugins();
      }
    });

    function cleanupOnDestroy() {
      DAGPlusPlusNodesActionsFactory.resetNodesAndConnections();
      DAGPlusPlusNodesStore.reset();

      if (subAvailablePlugins) {
        subAvailablePlugins();
      }

      angular.element($window).off('resize', vm.instance.repaintEverything);

      // Cancelling all timeouts, key bindings and event listeners
      Object.keys(repaintTimeoutsMap).forEach((id) => {
        $timeout.cancel(repaintTimeoutsMap[id]);
      });

      $timeout.cancel(commentsTimeout);
      $timeout.cancel(nodesTimeout);
      $timeout.cancel(fitToScreenTimeout);
      $timeout.cancel(initTimeout);
      $timeout.cancel(metricsPopoverTimeout);
      $timeout.cancel(highlightSelectedNodeConnectionsTimeout);
      Mousetrap.reset();
      dispatcher.unregister('onUndoActions', undoListenerId);
      dispatcher.unregister('onRedoActions', redoListenerId);
      vm.instance.reset();

      document.body.onpaste = null;
    }

    $scope.$on('$destroy', cleanupOnDestroy);
  });
