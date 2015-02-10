angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function ($filter) {

    var capitalize = $filter('caskCapitalizeFilter');

    return {
      restrict: 'E',

      scope: {
        model: '='
      },

      templateUrl: 'log-viewer/log-viewer.html',

      controller: function ($scope) {
        $scope.filters = 'all,info,warn,error,debug,other'.split(',')
          .map(function (type) {
            return {
              key: type,
              display: capitalize(type),
              entries: []
            };
          });

        $scope.activeFilter = 'all';

        $scope.setFilter = function (k) {
          $scope.activeFilter = k;
        };

        $scope.filterFn = function () {
          switch($scope.activeFilter) {
            case 'all':
            break;
          }
        };
      }
    };
  });
