angular.module(PKG.name + '.commons')
  .directive('mySqlQuery', function () {

    return {
      restrict: 'E',
      scope: {
        type: '=',
        name: '='
      },
      templateUrl: 'sql-query/sql-query.html',
      controller: function ($scope, $state, EventPipe, myExploreApi) {

        $scope.$watch('name', function() {
          $scope.query = 'SELECT * FROM ' + $scope.type + '_' + $scope.name + ' LIMIT 5';
        });

        $scope.execute = function() {
          var params = {
            namespace: $state.params.namespace,
            scope: $scope
          };

          myExploreApi.postQuery(params, { query: $scope.query },
            function () {
              EventPipe.emit('explore.newQuery');
            });

        };

      }
    };

  });
