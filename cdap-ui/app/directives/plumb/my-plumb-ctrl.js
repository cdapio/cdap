angular.module(PKG.name + '.commons')
  .controller('MyPlumbController', function MyPlumbController(jsPlumb, $scope, $timeout, MyPlumbService) {
    this.plugins = [];
    var instance;
    var defaultSettings = {
      Connector : [ "Flowchart" ],
      ConnectionsDetachable: true
    };
    var commonSettings = {
      endpoint:"Rectangle",
      paintStyle:{ width:10, height:10, fillStyle:'#666' },
      connectorOverlays: [ [ "Arrow", { location:1, width: 10, height: 10 } ] ]
    };
    var sourceSettings = angular.extend({
      isSource: true,
      anchor: 'Right'
    }, commonSettings);
    var sinkSettings = angular.extend({
      isTarget: true,
      anchor: 'Left'
    }, commonSettings);

    this.addPlugin = function addPlugin(config, type) {
      var id = 'plugin' + Date.now();
      this.plugins.push(angular.extend({
        type: type,
        id: id,
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

    MyPlumbService.registerCallBack(this.addPlugin.bind(this));

    jsPlumb.ready(function() {
      jsPlumb.setContainer('plumb-container');
      instance = jsPlumb.getInstance();
      instance.importDefaults(defaultSettings);
    });
  });
