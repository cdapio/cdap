angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsDetailController', function($scope, $state, MyDataSource, $alert) {

    var dataSrc = new MyDataSource($scope);

    $scope.truncate = function() {
      dataSrc.request({
        _cdapNsPath: '/data/datasets/' + $state.params.datasetId + '/admin/truncate',
        method: 'POST'
      }).then(function () {
        $alert({
          content: 'Succesfully truncated ' + $state.params.datasetId + ' dataset',
          type: 'success'
        });
      });
    };

  });
