angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamExploreController',
    function($scope, MyDataSource, $state, myHelpers, $log) {

      var dataSrc = new MyDataSource($scope);

      $scope.activePanel = 2;


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


      $scope.query = 'SELECT * from history LIMIT 5';

      $scope.execute = function() {
        dataSrc
          .request({
            _cdapNsPath: '/data/explore/queries',
            method: 'POST',
            body: {
              query: $scope.query
            }
          })
          .then(function () {
            $scope.getQueries();
          });
      };

      $scope.queries = [];

      $scope.getQueries = function() {
        dataSrc
          .request({
            _cdapNsPath: '/data/explore/queries',
            method: 'GET'
          })
          .then(function (queries) {
            $scope.queries = queries;
          });
      };

      $scope.getQueries();

    }
  );
