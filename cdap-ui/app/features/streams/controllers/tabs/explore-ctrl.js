angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamExploreController',
    function($scope, MyDataSource, $state, EventPipe) {

      var dataSrc = new MyDataSource($scope);

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
        dataSrc
          .request({
            _cdapNsPath: '/streams/' + $state.params.streamId +
              '/events?start=' + $scope.eventSearch.startMs +
              '&end=' + $scope.eventSearch.endMs +
              '&limit=' + $scope.eventSearch.limit
          }, function (result) {
            $scope.eventSearch.results = result;
          });
      };

      $scope.doEventSearch();

    }
  );
