angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetaiController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    $scope.flowletId = $state.params.flowletId;

  });
