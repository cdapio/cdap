angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetDetailProgramsController', function($scope, $state, myDatasetApi) {
    var params = {
      namespace: $state.params.namespace,
      datasetId: $state.params.datasetId,
      scope: $scope
    };
    myDatasetApi.programsList(params)
      .$promise
      .then(function(res) {
        this.programs = res;
      }.bind(this));
  });
