/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
  .controller('DAGPlusPlusCtrl', function MyDAGController(jsPlumb, $scope, $timeout, DAGPlusPlusFactory, GLOBALS, DAGPlusPlusNodesActionsFactory, $window, DAGPlusPlusNodesStore, $rootScope, $popover, uuid, DAGPlusPlusNodesDispatcher, HydratorPlusPlusDetailMetricsActions, NonStorePipelineErrorFactory, AvailablePluginsStore, myHelpers) {

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
    var endpointClicked = false;
    var connectionDropped = false;
    var dagMenu;
    let conditionNodes = [];
    let splitterNodesPorts = {};

    vm.pluginsMap = {};

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
    vm.dagMenuOpen = null;

    var repaintTimeout,
        commentsTimeout,
        nodesTimeout,
        fitToScreenTimeout,
        initTimeout,
        metricsPopoverTimeout,
        resetTimeout;

    var Mousetrap = window.CaskCommon.Mousetrap;

    function repaintEverything() {
      if (repaintTimeout) {
        $timeout.cancel(repaintTimeout);
      }

      repaintTimeout = $timeout(function () { vm.instance.repaintEverything(); });
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
        vm.instance.bind('connection', addConnection);
        vm.instance.bind('connectionDetached', removeConnection);
        vm.instance.bind('connectionMoved', moveConnection);
        vm.instance.bind('beforeStartDetach', onStartDetach);
        vm.instance.bind('beforeDrop', checkIfConnectionExistsOrValid);
        vm.instance.bind('beforeDrag', onBeforeDrag);
        // jsPlumb docs say the event for clicking on an endpoint is called 'endpointClick',
        // but seems like the 'click' event is triggered both when clicking on an endpoint &&
        // clicking on a connection
        vm.instance.bind('click', toggleConnections);

        Mousetrap.bind(['command+z', 'ctrl+z'], vm.undoActions);
        Mousetrap.bind(['command+shift+z', 'ctrl+shift+z'], vm.redoActions);
        Mousetrap.bind(['del', 'backspace'], vm.removeSelectedConnections);

        if (vm.isDisabled) {
          // Disable all endpoints
          angular.forEach($scope.nodes, function (node) {
            if (node.plugin.type === 'condition') {
              let endpoints = [`${node.name}_condition_true`, `${node.name}_condition_false`];
              angular.forEach(endpoints, (endpoint) => {
                disableEndpoints(endpoint);
              });
            } else if (node.plugin.type === 'splittertransform')  {
              let portNames = node.outputSchema.map(port => port.name);
              let endpoints = portNames.map(portName => `${node.name}_port_${portName}`);
              angular.forEach(endpoints, (endpoint) => {
                // different from others because the name here is the uuid of the splitter endpoint,
                // not the id of DOM element
                disableEndpoint(endpoint);
              });
            } else {
              disableEndpoints(node.name);
            }
          });
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

            angular.forEach($scope.metricsData, function (value, key) {
              metricsPopovers[key].scope.data.metrics = value;
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

    vm.nodeMouseEnter = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      var nodeInfo = metricsPopovers[node.name];

      if (metricsPopoverTimeout) {
        $timeout.cancel(metricsPopoverTimeout);
      }

      if (nodeInfo.element && nodeInfo.scope) {
        nodeInfo.popover = $popover(nodeInfo.element, {
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
      vm.instance.unmakeEverySource();
      vm.instance.unmakeEveryTarget();

      // This is to handle special case when we open a pipeline with only one node,
      // thought not sure why jsPlumb behaves differently in this case. Exclude
      // condition nodes since we add endpoints differently for those
      if ($scope.nodes.length === 1 && $scope.nodes[0].type !== 'condition') {
        vm.instance.deleteEveryEndpoint();
      }

      angular.forEach($scope.nodes, function (node) {
        if (node.type === 'condition') {
          initConditionNode(node.name);
        } else if (node.type === 'splittertransform') {
          initSplitterNode(node);
        } else {
          initNormalNode(node.name);
        }

        vm.instance.makeTarget(node.name, {
          isTarget: true,
          dropOptions: { hoverClass: 'drag-hover' },
          anchor: 'ContinuousLeft',
          allowLoopback: false
        });
      });
    }

    function initNormalNode(nodeName) {
      let sourceObj = {
        isSource: true,
        filter: function(event) {
          // we need this variable because when the user clicks on the endpoint circle, multiple
          // 'mousedown' events are fired
          if (event.target.className.indexOf('endpoint-circle') !== -1 && !endpointClicked) {
            endpointClicked = true;
          }

          return event.target.className.indexOf('endpoint-circle') !== -1;
        },
        connectionType: 'basic'
      };
      if (vm.isDisabled) {
        sourceObj.enabled = false;
      }

      vm.instance.makeSource(nodeName, sourceObj);
    }

    function initConditionNode(nodeName) {
      if (conditionNodes.indexOf(nodeName) !== -1) {
        return;
      }
      let trueEndpointDOMElId = nodeName + '_condition_true';
      let trueEndpointDOMEl = document.querySelector(`#${trueEndpointDOMElId}`);
      let newTrueEndpoint = vm.instance.addEndpoint(trueEndpointDOMElId, {
        anchor: 'Right',
        cssClass: 'condition-endpoint condition-endpoint-true',
        isSource: true,
        maxConnections: -1,
        endpoint: 'Dot',
        connectorStyle: vm.conditionTrueConnectionStyle,
        overlays: [
          [ 'Label', { label: 'Yes', id: 'yesLabel', location: [0.5, -0.55], cssClass: 'condition-label' } ]
        ]
      });
      newTrueEndpoint.hideOverlay('yesLabel');

      let falseEndpointDOMElId = nodeName + '_condition_false';
      let falseEndpointDOMEl = document.querySelector(`#${falseEndpointDOMElId}`);
      let newFalseEndpoint = vm.instance.addEndpoint(falseEndpointDOMElId, {
        anchor: 'Bottom',
        cssClass: 'condition-endpoint condition-endpoint-false',
        isSource: true,
        maxConnections: -1,
        endpoint: 'Dot',
        connectorStyle: vm.conditionFalseConnectionStyle,
        overlays: [
          [ 'Label', { label: 'No', id: 'noLabel', location: [0.5, -0.55], cssClass: 'condition-label' } ]
        ]
      });
      newFalseEndpoint.hideOverlay('noLabel');

      addListenersForEndpoint(newTrueEndpoint, trueEndpointDOMEl, 'yesLabel');
      addListenersForEndpoint(newFalseEndpoint, falseEndpointDOMEl, 'noLabel');

      conditionNodes.push(nodeName);
    }

    function initSplitterNode(node) {
      if (!node.outputSchema || !Array.isArray(node.outputSchema) || (Array.isArray(node.outputSchema) && node.outputSchema[0].name === GLOBALS.defaultSchemaName)) {
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
        let portElId = node.name + '_port_' + port;
        deleteEndpoints(portElId);
      });

      angular.forEach(node.outputSchema, (outputSchema) => {
        let domCircleElId = node.name + '_port_' + outputSchema.name;
        let splitterEndpoint;

        let domCircleEl = document.getElementsByClassName(domCircleElId);
        domCircleEl = domCircleEl[domCircleEl.length - 1];

        splitterEndpoint = vm.instance.addEndpoint(domCircleEl, {
          anchor: 'Right',
          cssClass: 'splitter-endpoint',
          isSource: true,
          maxConnections: -1,
          endpoint: 'Dot',
          uuid: domCircleElId
        });
        addListenersForEndpoint(splitterEndpoint, domCircleEl);
      });

      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
      splitterNodesPorts[node.name] = newPorts;
    }

    function getPortEndpointClass(splitterNodeClassList) {
      let portElemClassList = [].slice.call(splitterNodeClassList);
      let portClass = _.find(portElemClassList, (className) => className.indexOf('_port_') !== -1);
      return portClass;
    }

    function addConnections() {
      angular.forEach($scope.connections, function (conn) {
        var sourceNode = $scope.nodes.find(node => node.name === conn.from);
        var targetNode = $scope.nodes.find(node => node.name === conn.to);

        if (!sourceNode || !targetNode) {
          return;
        }

        let connObj = {
          source: conn.from,
          target: conn.to
        };

        if (conn.hasOwnProperty('condition')) {
          connObj.source = vm.instance.getEndpoints(`${conn.from}_condition_${conn.condition}`)[0];
        } else if (conn.hasOwnProperty('port')) {
          connObj.source = vm.instance.getEndpoint(`${conn.from}_port_${conn.port}`);
        }

        let newConn = vm.instance.connect(connObj);
        if (targetNode.type === 'condition' || sourceNode.type === 'action' || targetNode.type === 'action') {
          newConn.setType('dashed');
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

    function addConnection(newConnObj) {
      let connection = {
        from: newConnObj.sourceId,
        to: newConnObj.targetId
      };

      let sourceIsCondition = newConnObj.sourceId.indexOf('_condition_') !== -1;
      let sourceIsPort = newConnObj.source.className.indexOf('_port_') !== -1;

      let sourceIdSplit;

      if (sourceIsCondition || sourceIsPort) {
        if (sourceIsCondition) {
          sourceIdSplit = newConnObj.sourceId.split('_');
          connection.condition = sourceIdSplit[2] === 'true';
        } else {
          let portClass = getPortEndpointClass(newConnObj.source.classList);
          sourceIdSplit = portClass.split('_');
          connection.port = sourceIdSplit[2];
        }
        connection.from = sourceIdSplit[0];
      }

      $scope.connections.push(connection);
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    }

    function removeConnection(detachedConnObj, updateStore = true) {
      let connObj = Object.assign({}, detachedConnObj);
      if (detachedConnObj.sourceId.indexOf('_condition_') !== -1) {
        connObj.sourceId = detachedConnObj.sourceId.split('_')[0];
      } else if (detachedConnObj.source.className.indexOf('_port_') !== -1) {
        let portClass = getPortEndpointClass(detachedConnObj.source.classList);
        connObj.sourceId = portClass.split('_')[0];
      }
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
      // don't need to call addConnection for the new connection, since that will be done
      // automatically as part of the 'connection' event
      removeConnection(oldConnection, false);
    }

    vm.removeSelectedConnections = function() {
      if (selectedConnections.length === 0 || vm.isDisabled) { return; }

      vm.instance.unbind('connectionDetached');
      angular.forEach(selectedConnections, function (selectedConnectionObj) {
        removeContextMenuEventListener(selectedConnectionObj);
        removeConnection(selectedConnectionObj, false);
        vm.instance.detach(selectedConnectionObj);
      });
      vm.instance.bind('connectionDetached', removeConnection);
      selectedConnections = [];
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    };

    function toggleConnections(selectedObj) {
      if (vm.isDisabled) { return; }

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

      let connectionsToToggle;

      // If it's an endpoint on a condition node, then only toggle connections
      // from that endpoint. For other nodes, toggle all the connections of the
      // source that this endpoint belongs to

      if (selectedObj.endpoint.canvas.classList.contains('condition-endpoint')) {
        connectionsToToggle = selectedObj.connections;
      } else {
        connectionsToToggle = vm.instance.getConnections({
          source: selectedObj.connections[0].sourceId
        });
      }

      let notYetSelectedConnections = _.difference(connectionsToToggle, selectedConnections);

      // This is to toggle all connections coming from an endpoint.
      // If zero, one or more (but not all) of the connections are already selected,
      // then just select the remaining ones. Else if they're all selected,
      // then unselect them.

      if (notYetSelectedConnections.length !== 0) {
        notYetSelectedConnections.forEach(connection => {
          selectedConnections.push(connection);
          connection.connector.canvas.addEventListener('contextmenu', openContextMenu);
          connection.addType('selected');
        });
      } else {
        connectionsToToggle.forEach(connection => {
          selectedConnections.splice(selectedConnections.indexOf(connection), 1);
          removeContextMenuEventListener(connection);
          connection.removeType('selected');
        });
      }
    }

    function toggleConnection(connObj) {
      if (selectedConnections.indexOf(connObj) === -1) {
        selectedConnections.push(connObj);
        connObj.connector.canvas.addEventListener('contextmenu', openContextMenu);
      } else {
        selectedConnections.splice(selectedConnections.indexOf(connObj), 1);
        removeContextMenuEventListener(connObj);
      }
      connObj.toggleType('selected');
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

    function onStartDetach() {
      connectionDropped = false;
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

    function enableEndpointClickFlag() {
      endpointClicked = true;
    }

    function addListenersForEndpoint(endpoint, domCircleEl, labelId) {
      endpoint.canvas.removeEventListener('mouseover', addHoverListener);
      endpoint.canvas.removeEventListener('mouseout', removeHoverListener);
      endpoint.canvas.removeEventListener('mousedown', enableEndpointClickFlag);
      endpoint.canvas.addEventListener('mouseover', addHoverListener.bind(null, endpoint, domCircleEl, labelId));
      endpoint.canvas.addEventListener('mouseout', removeHoverListener.bind(null, endpoint, domCircleEl, labelId));
      endpoint.canvas.addEventListener('mousedown', enableEndpointClickFlag);
    }

    function checkIfConnectionExistsOrValid(connObj) {
      // return false if connection already exists, which will prevent the connecton from being formed
      if (connectionDropped) { return false; }

      if (connObj.sourceId.indexOf('_condition_') !== -1) {
        connObj.sourceId = connObj.sourceId.split('_')[0];
      } else if (connObj.connection.source.className.indexOf('_port_') !== -1) {
        let portClass = getPortEndpointClass(connObj.connection.source.classList);
        connObj.sourceId = portClass.split('_')[0];
      }

      var exists = _.find($scope.connections, function (conn) {
        return conn.from === connObj.sourceId && conn.to === connObj.targetId;
      });

      var sameNode = connObj.sourceId === connObj.targetId;

      if (exists || sameNode) {
        connectionDropped = true;
        return false;
      }

      // else check if the connection is valid
      var sourceNode = $scope.nodes.find( node => node.name === connObj.sourceId);
      var targetNode = $scope.nodes.find( node => node.name === connObj.targetId);

      var valid = true;

      NonStorePipelineErrorFactory.connectionIsValid(sourceNode, targetNode, function(invalidConnection) {
        if (invalidConnection) { valid = false; }
      });
      connectionDropped = true;


      if (!valid) {
        return valid;
      }

      // If valid, then modifies the look of the connection before showing it
      if (sourceNode.type === 'action' || targetNode.type === 'action') {
        connObj.connection.setType('dashed');
      } else if (sourceNode.type !== 'condition' && targetNode.type !== 'condition') {
        connObj.connection.setType('basic solid');
      } else {
        if (sourceNode.type === 'condition') {
          if (connObj.connection.endpoints && connObj.connection.endpoints.length > 0) {
            let sourceEndpoint = connObj.dropEndpoint;
            // TODO: simplify this?
            if (sourceEndpoint.canvas.classList.contains('condition-endpoint-true')) {
              connObj.connection.setType('conditionTrue');
            } else if (sourceEndpoint.canvas.classList.contains('condition-endpoint-false')) {
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

      return valid;
    }

    function onBeforeDrag() {
      if (endpointClicked) {
        endpointClicked = false;
        connectionDropped = false;
      }
    }

    function resetEndpointsAndConnections() {
      if (resetTimeout) {
        $timeout.cancel(resetTimeout);
      }

      resetTimeout = $timeout(function () {
        vm.instance.reset();
        conditionNodes = [];
        splitterNodesPorts = {};

        $scope.nodes = DAGPlusPlusNodesStore.getNodes();
        $scope.connections = DAGPlusPlusNodesStore.getConnections();
        vm.undoStates = DAGPlusPlusNodesStore.getUndoStates();
        vm.redoStates = DAGPlusPlusNodesStore.getRedoStates();
        makeNodesDraggable();
        initNodes();
        addConnections();
        angular.forEach(selectedConnections, function(selectedConnObj) {
          removeContextMenuEventListener(selectedConnObj);
        });
        selectedConnections = [];
        vm.instance.bind('connection', addConnection);
        vm.instance.bind('connectionMoved', moveConnection);
        vm.instance.bind('connectionDetached', removeConnection);
        vm.instance.bind('beforeDrop', checkIfConnectionExistsOrValid);
        vm.instance.bind('beforeStartDetach', onStartDetach);
        vm.instance.bind('beforeDrag', onBeforeDrag);
        vm.instance.bind('click', toggleConnections);

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

    function getPosition(e) {
      var posx = 0;
      var posy = 0;

      if (e.pageX || e.pageY) {
        posx = e.pageX;
        posy = e.pageY;
      } else if (e.clientX || e.clientY) {
        posx = e.clientX + document.body.scrollLeft + document.documentElement.scrollLeft;
        posy = e.clientY + document.body.scrollTop + document.documentElement.scrollTop;
      }

      return {
        x: posx,
        y: posy
      };
    }

    function positionContextMenu(e, menu) {
      var menuPosition = getPosition(e);
      menu.style.left = menuPosition.x + 'px';
      menu.style.top = menuPosition.y + 'px';
    }

    vm.selectEndpoint = function(event, node) {
      if (event.target.className.indexOf('endpoint-circle') === -1) { return; }

      endpointClicked = false;
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
    };

    function openContextMenu(e) {
      if (!selectedConnections.length) { return; }

      e.preventDefault();
      vm.openDagMenu(true);
      positionContextMenu(e, dagMenu);
    }

    function removeContextMenuEventListener(connection) {
      if (myHelpers.objectQuery(connection, 'connector', 'canvas')) {
        connection.connector.canvas.removeEventListener('contextmenu', openContextMenu);
      }
    }

    jsPlumb.ready(function() {
      var dagSettings = DAGPlusPlusFactory.getSettings();
      var dagSettingsDefault = dagSettings.default;
      var {defaultConnectionStyle, selectedConnectionStyle, dashedConnectionStyle, solidConnectionStyle, conditionTrueConnectionStyle, conditionFalseConnectionStyle} = dagSettings;
      vm.conditionTrueConnectionStyle = conditionTrueConnectionStyle;
      vm.conditionFalseConnectionStyle = conditionFalseConnectionStyle;

      jsPlumb.setContainer('dag-container');
      vm.instance = jsPlumb.getInstance(dagSettingsDefault);
      vm.instance.registerConnectionType('basic', defaultConnectionStyle);
      vm.instance.registerConnectionType('selected', selectedConnectionStyle);
      vm.instance.registerConnectionType('dashed', dashedConnectionStyle);
      vm.instance.registerConnectionType('solid', solidConnectionStyle);
      vm.instance.registerConnectionType('conditionTrue', conditionTrueConnectionStyle);
      vm.instance.registerConnectionType('conditionFalse', conditionFalseConnectionStyle);

      init();

      dagMenu = document.querySelector('.dag-popover-menu');

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
          });
        }
      }, true);

      // This is needed to redraw connections and endpoints on browser resize
      angular.element($window).on('resize', vm.instance.repaintEverything);

      DAGPlusPlusNodesStore.registerOnChangeListener(function () {
        vm.activeNodeId = DAGPlusPlusNodesStore.getActiveNodeId();

        // can do keybindings only if no node is selected
        if (!vm.activeNodeId) {
          Mousetrap.bind(['command+z', 'ctrl+z'], vm.undoActions);
          Mousetrap.bind(['command+shift+z', 'ctrl+shift+z'], vm.redoActions);
        } else {
          Mousetrap.unbind(['command+z', 'ctrl+z']);
          Mousetrap.unbind(['command+shift+z', 'ctrl+shift+z']);
        }
      });

    });

    vm.onNodeClick = function(event, node) {
      event.stopPropagation();
      closeMetricsPopover(node);
      HydratorPlusPlusDetailMetricsActions.setMetricsTabActive(false);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onMetricsClick = function(event, node) {
      event.stopPropagation();
      if ($scope.disableMetricsClick) {
        return;
      }
      closeMetricsPopover(node);
      HydratorPlusPlusDetailMetricsActions.setMetricsTabActive(true);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onNodeDelete = function (event, node) {
      event.stopPropagation();
      DAGPlusPlusNodesActionsFactory.removeNode(node.name);

      if (Object.keys(splitterNodesPorts).indexOf(node.name) !== -1) {
        delete splitterNodesPorts[node.name];
      }
      let nodeType = node.plugin.type || node.type;
      if (nodeType === 'splittertransform' && node.outputSchema && Array.isArray(node.outputSchema)) {
        let portNames = node.outputSchema.map(port => port.name);
        let endpoints = portNames.map(portName => `${node.name}_port_${portName}`);
        angular.forEach(endpoints, (endpoint) => {
          deleteEndpoints(endpoint);
        });
      }

      vm.instance.unbind('connectionDetached');
      selectedConnections = selectedConnections.filter(function(selectedConnObj) {
        if (selectedConnObj.sourceId === node.name || selectedConnObj.targetId === node.name) {
          removeContextMenuEventListener(selectedConnObj);
        }
        return selectedConnObj.sourceId !== node.name && selectedConnObj.targetId !== node.name;
      });
      vm.instance.unmakeSource(node.name);
      vm.instance.unmakeTarget(node.name);

      vm.instance.remove(node.name);
      vm.instance.bind('connectionDetached', removeConnection);
    };

    vm.cleanUpGraph = function () {
      if ($scope.nodes.length === 0) { return; }

      // If graph has a condition node, then have to make sure/rearrange connections from that
      // node so that the 'true' connection always comes before the 'false' one, otherweise
      // the algorithm will mess up
      if (conditionNodes.length > 0) {
        let swappedConnections = false;

        angular.forEach(conditionNodes, (conditionNode) => {
          let trueConnIndex = _.findIndex($scope.connections, (conn) => {
            return conn.from === conditionNode && conn.condition === true;
          });
          let falseConnIndex = _.findIndex($scope.connections, (conn) => {
            return conn.from === conditionNode && conn.condition === false;
          });

          if (trueConnIndex === -1 || falseConnIndex === -1 || trueConnIndex === falseConnIndex - 1) { return; }

          let falseConn = $scope.connections.splice(falseConnIndex, 1)[0];
          $scope.connections.splice(trueConnIndex + 1, 0, falseConn);
          swappedConnections = true;
        });

        if (swappedConnections) {
          DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
        }
      }

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

    vm.toggleNodeMenu = function (nodeName, event) {
      if (event) {
        event.preventDefault();
        event.stopPropagation();
      }

      if (vm.nodeMenuOpen === nodeName) {
        vm.nodeMenuOpen = null;
      } else {
        vm.nodeMenuOpen = nodeName;
      }
    };

    vm.openDagMenu = function(open) {
      vm.dagMenuOpen = open;
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
    });

    $scope.$on('$destroy', function () {
      DAGPlusPlusNodesActionsFactory.resetNodesAndConnections();
      DAGPlusPlusNodesStore.reset();

      subAvailablePlugins();

      angular.element($window).off('resize', vm.instance.repaintEverything);

      // Cancelling all timeouts, key bindings and event listeners
      $timeout.cancel(repaintTimeout);
      $timeout.cancel(commentsTimeout);
      $timeout.cancel(nodesTimeout);
      $timeout.cancel(fitToScreenTimeout);
      $timeout.cancel(initTimeout);
      $timeout.cancel(metricsPopoverTimeout);
      angular.forEach(selectedConnections, function(selectedConnObj) {
        removeContextMenuEventListener(selectedConnObj);
      });
      Mousetrap.reset();
      dispatcher.unregister('onUndoActions', undoListenerId);
      dispatcher.unregister('onRedoActions', redoListenerId);
      vm.instance.reset();
    });
  });
