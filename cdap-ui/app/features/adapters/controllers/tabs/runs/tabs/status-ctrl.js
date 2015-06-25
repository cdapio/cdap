angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, $state, myAdapterApi) {

    $scope.transforms = [{
      name: '',
      properties: {},
      type: ''
    }];
    $scope.source = {
      name: '',
      properties: {},
      type: ''
    };
    $scope.sink = {
      name: '',
      properties: {},
      type: ''
    };

    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms || [];
      });
});
