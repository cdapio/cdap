angular.module(PKG.name + '.commons')
  .directive('myDsv', function($window) {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        delimiter: '@'
      },
      templateUrl: 'widget-container/widget-dsv/widget-dsv.html',
      controller: function($scope) {

        var delimiter = $scope.delimiter || ',';

        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            return;
          }
          var arr = str.split(delimiter);

          angular.forEach(arr, function(a) {
            $scope.properties.push({
              value: a
            });
          });
        }

        initialize();

        var propertyListener = $scope.$watch('properties', function() {
          var str = '';

          angular.forEach($scope.properties, function(p) {
            if (p.value) {
              str = str + p.value + delimiter;
            }
          });

          // remove last delimiter
          if (str.length > 0 && str.charAt(str.length - 1) === delimiter ) {
            str = str.substring(0, str.length - 1);
          }

          $scope.model = str;

        }, true);

        $scope.$on('$destroy', function() {
          propertyListener();
        });

        $scope.addProperty = function() {
          $scope.properties.push({
            value: ''
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);
        };

      }
    };
  });
