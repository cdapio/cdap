angular.module(PKG.name + '.commons')
  .controller('MyDAGController', function MyDAGController(jsPlumb, $scope, $timeout, MyPlumbService, myHelpers, MyDAGFactory, $window, $popover, $rootScope, EventPipe) {
    this.plugins = $scope.config || [];
    this.isDisabled = $scope.isDisabled;
    MyPlumbService.setIsDisabled(this.isDisabled);

    var popovers = [];
    var popoverScopes = [];

    this.instance = null;

    this.addPlugin = function addPlugin(config, type) {
      closeAllPopovers();

      this.plugins.push(angular.extend({
        icon: MyDAGFactory.getIcon(config.name)
      }, config));
      $timeout(drawNode.bind(this, config.id, type));
      $timeout(this.instance.repaintEverything);
    };

    this.removePlugin = function(index, nodeId) {
      closeAllPopovers();

      this.instance.detachAllConnections(nodeId);
      this.instance.remove(nodeId);
      this.plugins.splice(index, 1);
      MyPlumbService.removeNode(nodeId);
      MyPlumbService.setConnections(this.instance.getConnections());
    };

    // Need to move this to the controller that is using this directive.
    this.onPluginClick = function(plugin) {
      closeAllPopovers();

      if (plugin.error) {
        delete plugin.error;
      }
      MyPlumbService.editPluginProperties($scope, plugin.id, plugin.type);
    };

    function errorNotification(errObj) {
      this.canvasError = [];
      if (errObj.canvas) {
        this.canvasError = errObj.canvas;
      }

      angular.forEach(this.plugins, function (plugin) {
        if (errObj[plugin.id]) {
          plugin.error = errObj[plugin.id];
        } else if (plugin.error) {
          delete plugin.error;
        }
      });
    }

    MyPlumbService.errorCallback(errorNotification.bind(this));

    this.closeCanvasError = function () {
      this.canvasError = [];
    };

    this.drawGraph = function() {
      var graph = MyDAGFactory.getGraph(this.plugins);
      var nodes = graph.nodes()
        .map(function(node) {
          return graph.node(node);
        });
      var margins, marginLeft;
      margins = $scope.getGraphMargins(this.plugins);
      marginLeft = margins.left;

      this.plugins.forEach(function(plugin) {
        plugin.icon = MyDAGFactory.getIcon(plugin.name);
        if (this.isDisabled) {
          plugin.style = plugin.style || MyDAGFactory.generateStyles(plugin.id, nodes, 0, marginLeft);
        } else {
          plugin.style = plugin.style || MyDAGFactory.generateStyles(plugin.id, nodes, 200, marginLeft);
        }
        drawNode.call(this, plugin.id, plugin.type);
      }.bind(this));

      drawConnections.call(this);

      mapSchemas.call(this);

      $timeout(this.instance.repaintEverything);
    };

    function drawNode(id, type) {
      var sourceSettings = MyDAGFactory.getSettings().source,
          sinkSettings = MyDAGFactory.getSettings().sink;

      switch(type) {
        case 'source':
          this.instance.addEndpoint(id, sourceSettings, {uuid: id});
          break;
        case 'sink':
          this.instance.addEndpoint(id, sinkSettings, {uuid: id});
          break;
        case 'transform':
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
        MyPlumbService.nodes[e.el.id].style = {top: e.el.style.top, left: e.el.style.left};
        return returnResult;
      }
    }

    function drawConnections() {
      var i;
      var curr, next;

      var connections = MyPlumbService.connections;
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
      var connections = MyPlumbService.connections;
      var nodes = MyPlumbService.nodes;
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
        scope.schema = MyPlumbService.formatSchema(MyPlumbService.nodes[connection.sourceId]);
        popover.show();
      });
    }

    MyPlumbService.registerCallBack(this.addPlugin.bind(this));

    $scope.$on('$destroy', function() {
      closeAllPopovers();
      angular.forEach(popoverScopes, function (s) {
        s.$destroy();
      });

      this.instance.reset();
      MyPlumbService.resetToDefaults();
    }.bind(this));

    jsPlumb.ready(function() {

      jsPlumb.setContainer('dag-container');
      this.instance = jsPlumb.getInstance();

      angular.element($window).on('resize', function() {
        this.instance.repaintEverything();
      }.bind(this));

      this.instance.importDefaults(MyDAGFactory.getSettings().default);

      // Need to move this to the controller that is using this directive.
      this.instance.bind('connection', function (con) {
        createPopover(con.connection);

        // Whenever there is a change in the connection just copy the entire array
        // We never know if a connection was altered or removed. We don't want to 'Sync'
        // between jsPlumb's internal connection array and ours (pointless)
        MyPlumbService.setConnections(this.instance.getConnections());
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

          createPopover(con.connection);

          MyPlumbService.setConnections(this.instance.getConnections());
        }.bind(this));
        this.instance.bind('connectionDetached', function(obj) {
          obj.connection.unbind('click');
          MyPlumbService.setConnections(this.instance.getConnections());
        }.bind(this));
        this.plugins = [];
        angular.forEach(MyPlumbService.nodes, function(node) {
          this.plugins.push(node);
        }.bind(this));
        $timeout(this.drawGraph.bind(this));
    }

    MyPlumbService.registerResetCallBack(resetComponent.bind(this));
    if (this.plugins.length) {
      resetComponent.call(this);
    }
  });
