angular.module(PKG.name + '.feature.streams')
  .controller('StreamProgramsController', function($scope, $state, myStreamApi) {

    var params = {
      namespace: $state.params.namespace,
      streamId: $state.params.streamId,
      scope: $scope
    };
    myStreamApi.programsList(params)
      .$promise
      .then(function (res) {
        this.programs = res;
      }.bind(this));

  });
