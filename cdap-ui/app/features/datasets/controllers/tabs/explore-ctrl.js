angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetExploreController',
    function($scope, MyDataSource, $state, myHelpers, $log) {

      var dataSrc = new MyDataSource($scope);

      $scope.activePanel = 0;

      dataSrc
        .request({
          _cdapNsPath: '/data/explore/tables'
        })
        .then(function (result) {
          console.log(result);
        });

    }
  );
