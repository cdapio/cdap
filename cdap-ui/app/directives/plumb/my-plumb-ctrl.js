angular.module(PKG.name + '.commons')
  .controller('MyPlumbController', function MyPlumbController(jsPlumb, $scope, $timeout, MyPlumbService) {
    this.plugins = $scope.config || [];
    this.instance = null;

    var defaultSettings = {
      Connector : [ "Flowchart" ],
      ConnectionsDetachable: true
    };

    var commonSettings = {
      endpoint:"Dot",
      paintStyle: {
          strokeStyle: "white",
          fillStyle: "#666",
          radius: 7,
          lineWidth: 3
      },
      connectorOverlays: [ [ "Arrow", { location:1, width: 10, height: 10 } ] ],
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

    this.onPluginClick = function(pluginId) {
      console.info('Plugin clicked: ', MyPlumbService.nodes[pluginId]);
    };

    this.removePlugin = function(index, nodeId) {
      this.instance.detachAllConnections(nodeId);
      this.instance.remove(nodeId);
      this.plugins.splice(index, 1);
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

      this.instance.bind("connection", function (connInfo, originalEvent) {
        MyPlumbService.updateConnection(this.instance.getConnections());
      }.bind(this));

    }.bind(this));
  });
