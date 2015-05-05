angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailProgramsController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/data/datasets/' + $state.params.datasetId + '/programs'
    }).then(function(res) {

      $scope.programs = res;

    });
  });
