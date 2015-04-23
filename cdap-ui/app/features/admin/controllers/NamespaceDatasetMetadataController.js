angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceDatasetMetadataController',
function ($scope, $state, $alert, MyDataSource) {

  var dataSrc = new MyDataSource($scope);

  dataSrc.request({
    _cdapPath: '/namespaces/' + $state.params.nsadmin
                  + '/data/explore/tables/dataset_' + $state.params.datasetId + '/info'
  }).then(function (res) {
    $scope.metadata = res;
  });

  $scope.deleteDataset = function() {
    dataSrc.request({
      _cdapPath: '/namespaces/' + $state.params.nsadmin +
                  '/data/datasets/' + $state.params.datasetId,
      method: 'DELETE'
    })
    .then(function () {
      $state.go('admin.namespace.detail.data', {}, {reload: true});
      $alert({
        type: 'success',
        content: 'Successfully deleted dataset'
      });
    });
  };

});
