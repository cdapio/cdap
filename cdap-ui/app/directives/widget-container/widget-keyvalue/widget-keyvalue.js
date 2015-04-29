angular.module(PKG.name + '.commons')
  .directive('myKeyValue', function($window) {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-keyvalue/widget-keyvalue.html',
      controller: function($scope) {

        var kvdelimiter = $scope.config['kv-delimiter'] || ':';
        var delimiter = $scope.config.delimiter || ',';

        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            return;
          }
          var arr = str.split(delimiter);

          angular.forEach(arr, function(a) {
            var split = a.split(kvdelimiter);

            $scope.properties.push({
              key: split[0],
              value: split[1]
            });
          });
        }

        initialize();

        var propertyListener = $scope.$watch('properties', function() {

          var str = '';

          angular.forEach($scope.properties, function(p) {
            if (p.key) {
              str = str + p.key + kvdelimiter + p.value + delimiter;
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
            key: '',
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
