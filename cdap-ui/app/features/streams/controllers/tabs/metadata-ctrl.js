angular.module(PKG.name + '.feature.streams')
  .controller('StreamMetadataController',
    function($scope, $state, myExploreApi) {

      var params = {
        namespace: $state.params.namespace,
        table: 'stream_' + $state.params.streamId,
        scope: $scope
      };
      this.metadata = {};
      
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.metadata = res;
        }.bind(this));

    }
  );
