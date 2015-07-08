angular.module(PKG.name + '.commons')
  .controller('MyPlumbController', function MyPlumbController(jsPlumb, $scope, $timeout, MyPlumbService) {
    this.plugins = [];
    var instance;
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
      var id = 'plugin' + Date.now();
      this.plugins.push(angular.extend({
        type: type,
        id: id,
        icon: getIcon(config.name)
      }, config));
      $timeout(function() {
        switch(type) {
          case 'source':
            instance.addEndpoint(id, sourceSettings);
            break;
          case 'sink':
            instance.addEndpoint(id, sinkSettings);
            break;
          case 'transform':
            instance.addEndpoint(id, sourceSettings);
            instance.addEndpoint(id, sinkSettings);
            break;
        }
        instance.draggable(id);
      });
    };

    this.removePlugin = function(index, nodeId) {
      instance.detachAllConnections(nodeId);
      instance.remove(nodeId);
      this.plugins.splice(index, 1);
    };

    MyPlumbService.registerCallBack(this.addPlugin.bind(this));

    jsPlumb.ready(function() {
      jsPlumb.setContainer('plumb-container');
      instance = jsPlumb.getInstance();
      instance.importDefaults(defaultSettings);
    });
  });
