angular.module(PKG.name + '.feature.apps')
  .controller('AppDetailStatusController', function($state, myAdapterApi, MyAppDAGService, CanvasFactory, GLOBALS, $scope) {
    this.nodes = [];
    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.appId,
      scope: $scope
    };

    this.cloneAdapter = function() {
      if (this.config) {
        $state.go('adapters.create', {
          data: this.config,
          type: this.config.artifact.name
        });
      }
    };

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        try{
          res.config = JSON.parse(res.configuration);
        } catch(e) {
          console.log('ERRPR in configuration from backend: ', e);
        }
        this.config = {
          name: $state.params.adapterId,
          template: res.artifact.name,
          description: res.description,
          config: {
            source: res.config.source,
            sink: res.config.sink,
            transforms: res.config.transforms,
            instances: res.instance,
            schedule: res.config.schedule
          }
        };

        MyAppDAGService.metadata.name = res.name;
        MyAppDAGService.metadata.description = res.description;
        MyAppDAGService.metadata.template.type = res.artifact.name;
        if (res.artifact.name === GLOBALS.etlBatch) {
          MyAppDAGService.metadata.template.schedule = res.config.schedule;
        } else if (res.artifact.name === GLOBALS.etlRealtime) {
          MyAppDAGService.metadata.template.instances = res.config.instances;
        }

        this.source = res.config.source;
        this.sink = res.config.sink;
        this.transforms = res.config.transforms;
        this.nodes = CanvasFactory.getNodes(res.config);
        this.nodes.forEach(function(node) {
          MyAppDAGService.addNodes(node, node.type);
        });

        MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes(this.nodes);


      }.bind(this));
  });
