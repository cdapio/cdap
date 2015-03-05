angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamExploreController',
    function($scope, MyDataSource, $state, myHelpers, $log) {

      var dataSrc = new MyDataSource($scope);

      $scope.activePanel = 0;

      dataSrc
        .poll({
          _cdapNsPath: '/streams/' + $state.params.streamId + '/events'
        }, function (result) {
          $scope.events = result;
        });

    }
  );
