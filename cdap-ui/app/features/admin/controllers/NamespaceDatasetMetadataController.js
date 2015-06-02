angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceDatasetMetadataController',
function ($scope, $state, $alert, $filter, myDatasetApi, myExploreApi) {

  var params = {
    namespace: $state.params.nsadmin,
    scope: $scope
  };

  myExploreApi.list(params)
    .$promise
    .then(function (tables) {
      var match = $filter('filter')(tables, $state.params.datasetId);
      if (match.length > 0) {
        params.table = 'dataset_' + $state.params.datasetId;

        myExploreApi.getInfo(params)
          .$promise
          .then(function (res) {
            $scope.metadata = res;
          });

      } else {
        $scope.metadata = null;
      }
    });


  $scope.deleteDataset = function() {
    var params = {
      namespace: $state.params.nsadmin,
      datasetId: $state.params.datasetId,
      scope: $scope
    };
    myDatasetApi.delete(params)
      .$promise
      .then(function () {
        $state.go('admin.namespace.detail.data', {}, {reload: true});
        $alert({
          type: 'success',
          content: 'Successfully deleted dataset'
        });
      });
  };

});
