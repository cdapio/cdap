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


      $scope.query = 'SELECT * FROM history LIMIT 5';

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
            $scope.activePanel = 2;
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

      $scope.results = {};

      $scope.fetchResult = function(query) {
        $scope.results.request = query;

        // request schema
        dataSrc
          .request({
            _cdapPath: '/data/explore/queries/' +
                          query.query_handle + '/schema'
          })
          .then(function (result) {
            $scope.results.schema = result;
          });

        // request preview
        dataSrc
          .request({
            _cdapPath: '/data/explore/queries/' +
                          query.query_handle + '/preview',
            method: 'POST'
          })
          .then(function (result) {
            $scope.results.results = result;
          });
      };

      $scope.download = function(query) {
        dataSrc
          .request({
            _cdapPath: '/data/explore/queries/' +
                            query.query_handle + '/download',
            method: 'POST'
          })
          .then(function (res) {
            var element = angular.element('<a/>');
            element.attr({
              href: 'data:atachment/csv,' + encodeURIComponent(res),
              target: '_self',
              download: 'result.csv'
            })[0].click();
          });
      };


    }
  );
