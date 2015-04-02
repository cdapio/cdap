angular.module(PKG.name + '.commons')
  .directive('mySqlQuery', function () {

    return {
      restrict: 'E',
      scope: {
        panel: '='
      },
      templateUrl: 'sql-query/sql-query.html',
      controller: function ($scope, MyDataSource, $state) {

        var dataSrc = new MyDataSource($scope);
        var current = '';
        if ($state.params.streamId) {
          current = 'stream_' + $state.params.streamId;
        } else {
          current = 'dataset_' + $state.params.datasetId;
        }

        $scope.query = "SELECT * FROM " + current;

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
              // $scope.getQueries();
              $scope.panel = 2;
            });
        };

      }
    };

  });
