angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetExploreController',
    function($scope, MyDataSource, QueryModel, $state, myHelpers, $log) {
      

      var dataSrc = new MyDataSource($scope);
      var dataModel = new QueryModel(dataSrc, 'exploreQueries');

      $scope.activePanel = 0;

      dataSrc
        .request({
          _cdapNsPath: '/data/explore/tables'
        })
        .then(function (result) {
          $scope.tables = result;
        });


      // EXECUTE QUERY
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
        dataModel.get()
          .then(function (queries) {
            $scope.queries = queries;
          });
      };



      // FETCHING QUERIES
      $scope.getQueries();

      $scope.responses = {};

      $scope.fetchResult = function(query) {
        $scope.responses.request = query;

        // request schema
        dataSrc
          .request({
            _cdapPath: '/data/explore/queries/' +
                          query.query_handle + '/schema'
          })
          .then(function (result) {
            $scope.responses.schema = result;
          });

        // request preview
        dataSrc
          .request({
            _cdapPath: '/data/explore/queries/' +
                          query.query_handle + '/preview',
            method: 'POST'
          })
          .then(function (result) {
            $scope.responses.results = result;
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
