angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceDatasetMetadataController',
function ($scope, $state, $alert, MyDataSource, $filter, myDatasetApi) {

  var dataSrc = new MyDataSource($scope);

  dataSrc.request({
    _cdapPath: '/namespaces/' + $state.params.nsadmin
                  + '/data/explore/tables'
  }).then(function (tables) {
    var match = $filter('filter')(tables, $state.params.datasetId);
    if (match.length > 0) {
      dataSrc.request({
        _cdapPath: '/namespaces/' + $state.params.nsadmin
                      + '/data/explore/tables/dataset_' + $state.params.datasetId + '/info'
      }).then(function (res) {
        $scope.metadata = res;
      });
    } else {
      $scope.metadata = null;
    }
  });



  $scope.deleteDataset = function() {
    // dataSrc.request({
    //   _cdapPath: '/namespaces/' + $state.params.nsadmin +
    //               '/data/datasets/' + $state.params.datasetId,
    //   method: 'DELETE'
    // })
    // .then(function () {
    //   $state.go('admin.namespace.detail.data', {}, {reload: true});
    //   $alert({
    //     type: 'success',
    //     content: 'Successfully deleted dataset'
    //   });
    // });
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
