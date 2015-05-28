angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamExploreController',
    function($scope, $state, EventPipe, myStreamApi) {

      $scope.activePanel = [0];
      $scope.name = $state.params.streamId;

      EventPipe.on('explore.newQuery', function() {
        $scope.activePanel = [0,1,2];
      });

      var now = Date.now();

      $scope.eventSearch = {
        startMs: now-(60*60*1000*2), // two hours ago
        endMs: now,
        limit: 10,
        results: []
      };

      $scope.doEventSearch = function () {
        var params = {
          namespace: $state.params.namespace,
          streamId: $state.params.streamId,
          scope: $scope,
          start: $scope.eventSearch.startMs,
          end: $scope.eventSearch.endMs,
          limit: $scope.eventSearch.limit
        };
        myStreamApi.eventSearch(params)
          .$promise
          .then(function (res) {
            $scope.eventSearch.results = res;
          });
      };

      $scope.doEventSearch();

    }
  );
