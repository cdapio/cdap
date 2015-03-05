angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamExploreController',
    function($scope, MyDataSource, $state, myHelpers, $log) {

      var dataSrc = new MyDataSource($scope);

      $scope.activePanel = 0;

      dataSrc
        .request({
          _cdapNsPath: '/data/explore/tables'
        })
        .then(function (result) {
          $scope.tables = result;
        });

    }
  );
