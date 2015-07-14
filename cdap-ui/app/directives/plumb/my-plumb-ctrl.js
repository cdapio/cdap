angular.module(PKG.name + '.commons')
  .controller('MyPlumbController', function MyPlumbController(jsPlumb, $scope, $timeout, MyPlumbService, myHelpers, AdapterErrorFactory) {
    this.plugins = $scope.config || [];
    this.instance = null;
    // this.nodesErrors = AdapterErrorFactory.nodesError;
    // this.canvasError = AdapterErrorFactory.canvasError;

    var defaultSettings = {
      Connector : [ 'Flowchart' ],
      ConnectionsDetachable: true
    };
    var commonSettings = {
      endpoint:'Dot',
      maxConnections: -1,
      paintStyle: {
          strokeStyle: 'white',
          fillStyle: '#666',
          radius: 7,
          lineWidth: 3
      },
      connectorOverlays: [ [ 'Arrow', { location:1, width: 10, height: 10 } ] ],
      anchors: [ 'Perimeter', {shape: 'Circle'}]
    };
    var sourceSettings = angular.extend({
      isSource: true,
      anchor: 'Right'
    }, commonSettings);
    var sinkSettings = angular.extend({
      isTarget: true,
      anchor: 'Left'
    }, commonSettings);

    function getIcon(plugin) {
      var iconMap = {
        'script': 'fa-code',
        'twitter': 'fa-twitter',
        'cube': 'fa-cubes',
        'data': 'fa-database',
        'database': 'fa-database',
        'table': 'fa-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-plugin-stream',
        'avro': 'icon-avro',
        'jms': 'icon-jms'
      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    this.addPlugin = function addPlugin(config, type) {
      this.plugins.push(angular.extend({
        icon: getIcon(config.name)
      }, config));
      $timeout(drawNode.bind(this, config.id, type));
      $timeout(this.instance.repaintEverything);
    };

    function drawNode(id, type) {
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
      this.instance.draggable(id);
    }

    this.removePlugin = function(index, nodeId) {
      this.instance.detachAllConnections(nodeId);
      this.instance.remove(nodeId);
      this.plugins.splice(index, 1);
      MyPlumbService.removeNode(nodeId);
      MyPlumbService.setConnections(this.instance.getConnections());
    };

    // Need to move this to the controller that is using this directive.
    this.onPluginClick = function(plugin) {
      if (plugin.error) {
        delete plugin.error;
      }
      MyPlumbService.editPluginProperties($scope, plugin.id, plugin.type);
    };

    function errorNotification(errObj) {
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


    MyPlumbService.registerCallBack(this.addPlugin.bind(this));
    MyPlumbService.errorCallback(errorNotification.bind(this));

    // Using Dagre here to generate x and y co-ordinates for each node.
    // When we fork and branch and have complex connections this will be useful for us.
    // Right now this returns a pretty simple straight linear graph.
    this.getGraph = function() {
      var graph = new dagre.graphlib.Graph();
      graph.setGraph({
        nodesep: 60,
        ranksep: 100,
        rankdir: 'LR',
        marginx: 30,
        marginy: 30
      });
      this.plugins.forEach(function(plugin) {
        graph.setNode(plugin.id, {label: plugin.id, width: 100, height: 100});
      });
      dagre.layout(graph);
      return graph;
    };

    function generateStyles(name, nodes, xmargin, ymargin) {
      var styles = {};
      var nodeStylesFromDagre = nodes.filter(function(node) {
        return node.label === name;
      });
      if (nodeStylesFromDagre.length) {
        styles = {
          'top': (nodeStylesFromDagre[0].x + xmargin) + 'px',
          'left': (nodeStylesFromDagre[0].y + ymargin) + 'px'
        };
      }
      return styles;
    }

    function drawConnections() {
      var i;
      var prev, curr, next;
      for(i=0; i<this.plugins.length-1; i++) {
        if (this.plugins[i].type === 'transform') {
          curr = 'Left' + this.plugins[i].id;
        } else {
          curr = this.plugins[i].id;
        }
        if (this.plugins[i+1].type === 'transform') {
          next = 'Right' + this.plugins[i+1].id;
        } else {
          next = this.plugins[i+1].id;
        }
        this.instance.connect({
          uuids: [curr, next],
          editable: true
        });
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

    this.drawGraph = function() {
      var graph = this.getGraph();
      var nodes = graph.nodes()
        .map(function(node) {
          return graph.node(node);
        });

      this.plugins.forEach(function(plugin) {
        plugin.icon = getIcon(plugin.name);
        plugin.style = generateStyles(plugin.id, nodes, 200, 300);
        drawNode.call(this, plugin.id, plugin.type);
      }.bind(this));

      drawConnections.call(this);

      MyPlumbService.setConnections(this.instance.getConnections());

      mapSchemas.call(this);

      $timeout(this.instance.repaintEverything);
    };

    $scope.$on('$destroy', function() {
      this.instance.reset();
      MyPlumbService.resetToDefaults();
    }.bind(this));

    jsPlumb.ready(function() {
      jsPlumb.setContainer('plumb-container');
      this.instance = jsPlumb.getInstance();

      this.instance.importDefaults(defaultSettings);

      // Need to move this to the controller that is using this directive.
      this.instance.bind('connection', function () {
        // Whenever there is a change in the connection just copy the entire array
        // We never know if a connection was altered or removed. We don't want to 'Sync'
        // between jsPlumb's internal connection array and ours (pointless)
        MyPlumbService.setConnections(this.instance.getConnections());
      }.bind(this));

      if (this.plugins.length > 0) {
        $timeout(this.drawGraph.bind(this));
      }
    }.bind(this));

  });
