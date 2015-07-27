angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, $state, myAdapterApi, $bootstrapModal, CanvasFactory, MyPlumbService) {

    $scope.nodes = [];
    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    var template;

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms;
        $scope.nodes = CanvasFactory.getNodes(res.config);
        $scope.nodes.forEach(function(node) {
          MyPlumbService.addNodes(node, node.type);
        });

        MyPlumbService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes);

        MyPlumbService.metadata.name = res.name;
        MyPlumbService.metadata.description = res.description;
        MyPlumbService.metadata.template.type = res.template;
        if (res.template === 'ETLBatch') {
          MyPlumbService.metadata.template.instances = res.instances;
        }

      });
  });
