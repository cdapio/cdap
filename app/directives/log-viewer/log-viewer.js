angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function ($filter, $timeout) {

    var capitalize = $filter('caskCapitalizeFilter'),
        filterFilter = $filter('filter');

    return {
      restrict: 'E',

      scope: {
        model: '='
      },

      templateUrl: 'log-viewer/log-viewer.html',

      controller: function ($scope) {

        $scope.filters = 'all,info,warn,error,debug,other'.split(',')
          .map(function (key) {
            var p;
            switch(key) {
              case 'all':
                p = function() { return true; };
                break;
              case 'other':
                p = function(line) { return !(/- (INFO|WARN|ERROR|DEBUG)/).test(line.log); };
                break;
              default:
                p = function(line) { return (new RegExp('- '+key.toUpperCase())).test(line.log); };
            }
            return {
              key: key,
              label: capitalize(key),
              entries: [],
              predicate: p
            };
          });

        $scope.$watch('model', function (newVal) {
          angular.forEach($scope.filters, function (one) {
            one.entries = filterFilter($scope.model, one.predicate);
          });
        });

      },

      link: function (scope, element, attrs) {
        var termEl = angular.element(element[0].querySelector('.terminal'));

        scope.setFilter = function (k) {

          scope.activeFilter = filterFilter(scope.filters, {key:k})[0];

          $timeout(function(){
            termEl.prop('scrollTop', termEl.prop('scrollHeight'));
          });

        };

        scope.setFilter('all');
      }
    };
  });

