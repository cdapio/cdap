angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamMetadataController',
    function($scope, $state, myExploreApi) {

      var params = {
        namespace: $state.params.namespace,
        table: 'stream_' + $state.params.streamId,
        scope: $scope
      };

      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          $scope.metadata = res;
        });

    }
  );
