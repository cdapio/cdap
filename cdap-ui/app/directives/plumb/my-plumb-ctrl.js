angular.module(PKG.name + '.commons')
  .controller('MyPlumbController', function MyPlumbController(jsPlumb, $scope, $timeout, MyPlumbService, AdapterErrorFactory) {
    this.plugins = $scope.config || [];
    this.instance = null;
    this.nodesErrors = AdapterErrorFactory.nodesError;
    this.canvasError = AdapterErrorFactory.canvasError;

    var defaultSettings = {
      Connector : [ 'Flowchart' ],
      ConnectionsDetachable: true
    };
    var commonSettings = {
      endpoint:'Dot',
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
    };

    function drawNode(id, type) {
      switch(type) {
        case 'source':
          this.instance.addEndpoint(id, sourceSettings);
          break;
        case 'sink':
          this.instance.addEndpoint(id, sinkSettings);
          break;
        case 'transform':
          this.instance.addEndpoint(id, sourceSettings);
          this.instance.addEndpoint(id, sinkSettings);
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
    this.onPluginClick = function(pluginId, pluginType) {
      MyPlumbService.editPluginProperties($scope, pluginId, pluginType);
    };

    MyPlumbService.registerCallBack(this.addPlugin.bind(this));

    jsPlumb.ready(function() {
      jsPlumb.setContainer('plumb-container');
      this.instance = jsPlumb.getInstance();

      this.instance.importDefaults(defaultSettings);

      this.plugins.forEach(function(plugin) {
        drawNode.call(this, plugin.id, plugin.type);
        // Have to add Connections.
      }.bind(this));

      // Need to move this to the controller that is using this directive.
      this.instance.bind('connection', function () {
        // Whenever there is a change in the connection just copy the entire array
        // We never know if a connection was altered or removed. We don't want to 'Sync'
        // between jsPlumb's internal connection array and ours (pointless)
        MyPlumbService.setConnections(this.instance.getConnections());
      }.bind(this));

    }.bind(this));

  });
