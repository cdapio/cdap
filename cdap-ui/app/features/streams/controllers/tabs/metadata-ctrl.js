angular.module(PKG.name + '.feature.datasets')
  .controller('CdapStreamMetadataController',
    function($scope, MyDataSource, $state, myHelpers) {

      var dataSrc = new MyDataSource($scope);

      dataSrc.request({
        _cdapNsPath: '/data/explore/tables/stream_' + $state.params.streamId + '/info'
      })
      .then(function(res) {
        $scope.metadata = res;
      });

    }
  );
