angular.module(PKG.name + '.commons')
  .directive('mySqlQuery', function () {

    return {
      restrict: 'E',
      scope: {
        panel: '=',
        type: '=',
        name: '='
      },
      templateUrl: 'sql-query/sql-query.html',
      controller: function ($scope, MyDataSource, $state) {

        var dataSrc = new MyDataSource($scope);

        $scope.$watch('name', function(newVal) {
          $scope.query = 'SELECT * FROM ' + $scope.type + '_' + $scope.name + ' LIMIT 5';
        });

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
              $scope.panel++;
            });
        };

      }
    };

  });
