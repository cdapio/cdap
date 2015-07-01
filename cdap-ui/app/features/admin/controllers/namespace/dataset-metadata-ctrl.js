angular.module(PKG.name + '.feature.admin').controller('NamespaceDatasetMetadataController',
function ($scope, $state, $alert, $filter, myDatasetApi, myExploreApi, EventPipe) {

  var params = {
    namespace: $state.params.nsadmin,
    scope: $scope
  };

  myExploreApi.list(params)
    .$promise
    .then(function (tables) {

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');

      var match = $filter('filter')(tables, datasetId);
      if (match.length > 0) {

        params.table = 'dataset_' + datasetId;

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
    EventPipe.emit('showLoadingIcon');
    var params = {
      namespace: $state.params.nsadmin,
      datasetId: $state.params.datasetId,
      scope: $scope
    };
    myDatasetApi.delete(params, {}, function success() {
      EventPipe.emit('hideLoadingIcon.immediate');

      $state.go('admin.namespace.detail.data', {}, {reload: true});
      $alert({
        type: 'success',
        content: 'Successfully deleted dataset'
      });
    }, function error() {
      EventPipe.emit('hideLoadingIcon.immediate');
    });
  };

});
