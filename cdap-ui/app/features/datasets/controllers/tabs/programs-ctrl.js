angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailProgramsController', function($scope, $state, myDatasetApi) {
    var params = {
      namespace: $state.params.namespace,
      datasetId: $state.params.datasetId,
      scope: $scope
    };
    myDatasetApi.programsList(params)
      .$promise
      .then(function(res) {
        $scope.programs = res;
      });
  });
