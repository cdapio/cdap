angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamDetailController', function($scope, $state, $alert, myStreamService, myStreamApi) {

    this.truncate = function() {
      var params = {
        namespace: $state.params.namespace,
        streamId: $state.params.streamId,
        scope: $scope
      };
      myStreamApi.truncate(params, {})
        .$promise
        .then(function () {
          $alert({
            type: 'success',
            content: 'Successfully truncated ' + $state.params.streamId + ' stream'
          });
        });
    };

    this.sendEvents = function() {
      myStreamService.show($state.params.streamId);
    };

  });
