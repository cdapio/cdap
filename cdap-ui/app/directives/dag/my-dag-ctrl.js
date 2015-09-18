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
  .controller('MyDAGController', function MyDAGController(jsPlumb, $scope, $timeout, MyAppDAGService, myHelpers, MyDAGFactory, $window, $popover, $rootScope, EventPipe, GLOBALS, MyNodeConfigService, AdapterErrorFactory) {
    this.plugins = $scope.config || [];
    this.MyAppDAGService = MyAppDAGService;
    this.isDisabled = $scope.isDisabled;
    MyAppDAGService.setIsDisabled(this.isDisabled);

    var popovers = [];
    var popoverScopes = [];

    this.instance = null;

    this.resetPluginSelection = function(plugin) {
      angular.forEach(this.plugins, function(plug) {
        plug.selected = false;
        if (plug.id === plugin.id) {
          plug.selected = true;
        }
        this.highlightRequiredFields(plug);
      }.bind(this));
    };

    this.addPlugin = function addPlugin(config, type) {
      closeAllPopovers();
      var plugin;
      this.plugins.push(angular.extend({
        icon: MyDAGFactory.getIcon(config.name)
      }, config));

      plugin = this.plugins[this.plugins.length-1];
      this.highlightRequiredFields(plugin);
      $timeout(drawNode.bind(this, config.id, type));
      $timeout(this.instance.repaintEverything);
    };

    this.highlightRequiredFields = function(plugin) {
      if (!angular.isObject(plugin)) {
        angular.forEach(MyAppDAGService.nodes, function(plug, pluginId) {
          if (plugin === pluginId) {
            plugin = plug;
          }
        });
      }

      AdapterErrorFactory.isValidPlugin(plugin);

      this.plugins.forEach(function(p) {
        if (p.id === plugin.id) {
          if (plugin.valid) {
            p.requiredFieldCount = 0;
            p.error = false;
            p.warning = false;
          } else {
            p.requiredFieldCount = plugin.requiredFieldCount;
            p.error = {};
            p.error.message = 'Missing required fields';
            p.warning = true;
          }
        }
      });
    };

    this.removePlugin = function(index, nodeId) {
      closeAllPopovers();

      this.instance.detachAllConnections(nodeId);
      this.instance.remove(nodeId);
      this.plugins.splice(index, 1);
      MyAppDAGService.removeNode(nodeId);
      MyAppDAGService.setConnections(this.instance.getConnections());
      MyNodeConfigService.removePlugin(nodeId);
    };

    // Need to move this to the controller that is using this directive.
    this.onPluginClick = function(plugin) {
      closeAllPopovers();
      angular.forEach(this.plugins, function(plug) {
        plug.selected = false;
      });

      plugin.selected = true;
      MyAppDAGService.editPluginProperties($scope, plugin.id, plugin.type);
    };

    function errorNotification(errObj) {
      this.canvasError = [];
      if (errObj.canvas) {
        this.canvasError = errObj.canvas;
      }

      angular.forEach(this.plugins, function (plugin) {
        if (errObj[plugin.id]) {
          plugin.error = errObj[plugin.id];
          plugin.warning = false;
        } else if (plugin.error) {
          // The nodes could just lie there on the canvas without connected.
          // In such cases there might still be some required fields not set in those nodes.
          // we would still want to show them as a warning.
          AdapterErrorFactory.isValidPlugin(plugin);
          if (plugin.valid) {
            delete plugin.error;
          } else {
            plugin.warning = false;
          }
        }
      });
    }

    MyAppDAGService.errorCallback(errorNotification.bind(this));

    this.closeCanvasError = function () {
      this.canvasError = [];
    };

    this.drawGraph = function() {
      var graph = MyDAGFactory.getGraph(this.plugins, MyAppDAGService.metadata.template.type);
      var nodes = graph.nodes()
        .map(function(node) {
          return graph.node(node);
        });
      var margins, marginLeft;
      margins = $scope.getGraphMargins(this.plugins);
      marginLeft = margins.left;
      this.instance.endpointAnchorClassPrefix = '';
      this.plugins.forEach(function(plugin) {
        plugin.icon = MyDAGFactory.getIcon(plugin.name);
        if (this.isDisabled) {
          plugin.style = plugin.style || MyDAGFactory.generateStyles(plugin.id, nodes, marginLeft, 0);
        } else {
          plugin.style = plugin.style || MyDAGFactory.generateStyles(plugin.id, nodes, marginLeft, 200);
        }
        drawNode.call(this, plugin.id, plugin.type);
      }.bind(this));

      drawConnections.call(this);

      mapSchemas.call(this);

      $timeout(this.instance.repaintEverything);
    };

    function drawNode(id, type) {
      var sourceSettings = angular.copy(MyDAGFactory.getSettings().source),
          sinkSettings = angular.copy(MyDAGFactory.getSettings().sink);
      var artifactType = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type];
      switch(type) {
        case artifactType.source:
          this.instance.addEndpoint(id, sourceSettings, {uuid: id});
          break;
        case artifactType.sink:
          this.instance.addEndpoint(id, sinkSettings, {uuid: id});
          break;
        case artifactType.transform:
          sourceSettings.anchor = [ 0.5, 1, 0, 0, 26, -43, 'transformAnchor'];
          sinkSettings.anchor = [ 0.5, 1, 0, 0, -26, -43, 'transformAnchor'];
          // Need to id each end point so that it can be used later to make connections.
          this.instance.addEndpoint(id, sourceSettings, {uuid: 'Left' + id});
          this.instance.addEndpoint(id, sinkSettings, {uuid: 'Right' + id});
          break;
      }
      if (!this.isDisabled) {
        this.instance.draggable(id, {
          drag: function (evt) { return dragnode.call(this, evt); }.bind(this)
        });
      }
      // Super hacky way of restricting user to not scroll beyond certain top and left.
      function dragnode(e) {
        closeAllPopovers();

        var returnResult = true;
        if (e.pos[1] < 0) {
          e.e.preventDefault();
          e.el.style.top = '10px';
          returnResult = false;
        }
        if (e.pos[0] < 0) {
          e.e.preventDefault();
          e.el.style.left = '10px';
          returnResult = false;
        }
        MyAppDAGService.nodes[e.el.id].style = {top: e.el.style.top, left: e.el.style.left};
        return returnResult;
      }
    }

    function drawConnections() {
      var i;
      var curr, next;

      var connections = MyAppDAGService.connections;
      for(i=0; i<connections.length; i++) {
        if (connections[i].source.indexOf('transform') !== -1) {
          curr = 'Left' + connections[i].source;
        } else {
          curr = connections[i].source;
        }
        if (connections[i].target.indexOf('transform') !== -1) {
          next = 'Right' + connections[i].target;
        } else {
          next = connections[i].target;
        }

        var connObj = {
          uuids: [curr, next]
        };

        if (this.isDisabled) {
          connObj.detachable = false;
        }
        this.instance.connect(connObj);
      }
    }

    function mapSchemas() {
      var connections = MyAppDAGService.connections;
      var nodes = MyAppDAGService.nodes;
      connections.forEach(function(connection) {
        var sourceNode = nodes[connection.source];
        var targetNode = nodes[connection.target];
        var sourceOutputSchema = myHelpers.objectQuery(sourceNode, 'properties', 'schema');
        var targetOuputSchema = myHelpers.objectQuery(targetNode, 'properties', 'schema');
        if (sourceOutputSchema) {
          sourceNode.outputSchema = sourceOutputSchema;
        }
        if (targetOuputSchema) {
          targetNode.outputSchema = targetOuputSchema;
        } else {
          targetNode.outputSchema = sourceNode.outputSchema;
        }
      });
    }

    function closeAllPopovers() {
      angular.forEach(popovers, function (popover) {
        popover.hide();
      });
    }

    EventPipe.on('popovers.close', function () {
      closeAllPopovers();
    });

    EventPipe.on('popovers.reset', function () {
      closeAllPopovers();

      popovers = [];

      angular.forEach(popoverScopes, function (s) {
        s.$destroy();
      });

    });

    function createPopover(connection) {
      var label = angular.element(connection.getOverlay('label').getElement());

      var scope = $rootScope.$new();
      popoverScopes.push(scope);

      var popover = $popover(label, {
        trigger: 'manual',
        placement: 'auto',
        target: label,
        template: '/assets/features/adapters/templates/partial/schema-popover.html',
        container: 'main',
        scope: scope
      });

      popovers.push(popover);

      connection.bind('click', function () {
        scope.schema = MyAppDAGService.formatSchema(MyAppDAGService.nodes[connection.sourceId]);
        popover.show();
      });
    }

    $scope.$on('$destroy', function() {
      MyNodeConfigService.unRegisterPluginResetCallback($scope.$id);
      MyNodeConfigService.unRegisterPluginSaveCallback($scope.$id);
      closeAllPopovers();
      angular.forEach(popoverScopes, function (s) {
        s.$destroy();
      });

      this.instance.reset();
      MyAppDAGService.resetToDefaults();
    }.bind(this));

    jsPlumb.ready(function() {

      jsPlumb.setContainer('dag-container');
      this.instance = jsPlumb.getInstance();
      // Overrides JSPlumb's prefixed endpoint classes. This variable will be changing in the next version of JSPlumb
      this.instance.endpointAnchorClassPrefix = '';

      angular.element($window).on('resize', function() {
        this.instance.repaintEverything();
      }.bind(this));

      this.instance.importDefaults(MyDAGFactory.getSettings().default);

      // Need to move this to the controller that is using this directive.
      this.instance.bind('connection', function (con) {
        if (!this.isDisabled) {
          createPopover(con.connection);
        }

        // Whenever there is a change in the connection just copy the entire array
        // We never know if a connection was altered or removed. We don't want to 'Sync'
        // between jsPlumb's internal connection array and ours (pointless)
        MyAppDAGService.setConnections(this.instance.getConnections());
      }.bind(this));
    }.bind(this));

    function resetComponent() {

        angular.forEach(this.instance.getConnections(), function (connection) {
          connection.unbind('click');
        });
        popovers = [];

        this.instance.reset();
        this.instance = jsPlumb.getInstance();
        this.instance.importDefaults(MyDAGFactory.getSettings().default);
        this.instance.bind('connection', function (con) {
          if (!this.isDisabled) {
            createPopover(con.connection);
          }

          MyAppDAGService.setConnections(this.instance.getConnections());
        }.bind(this));
        this.instance.bind('connectionDetached', function(obj) {
          obj.connection.unbind('click');
          MyAppDAGService.setConnections(this.instance.getConnections());
        }.bind(this));
        this.plugins = [];
        angular.forEach(MyAppDAGService.nodes, function(node) {
          this.plugins.push(node);
          if (node._backendProperties) {
            this.highlightRequiredFields(this.plugins[this.plugins.length -1]);
          }
        }.bind(this));
        $timeout(this.drawGraph.bind(this));
    }

    if (this.plugins.length) {
      resetComponent.call(this);
    }

    MyNodeConfigService.registerPluginResetCallback($scope.$id, this.resetPluginSelection.bind(this));
    MyNodeConfigService.registerPluginSaveCallback($scope.$id, this.highlightRequiredFields.bind(this));
    MyAppDAGService.registerCallBack(this.addPlugin.bind(this));
    MyAppDAGService.registerResetCallBack(resetComponent.bind(this));
  });
