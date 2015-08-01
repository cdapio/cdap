angular.module(PKG.name + '.commons')
  .directive('myKeyValue', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-keyvalue/widget-keyvalue.html',
      controller: function($scope, EventPipe) {
        var modelCopy = angular.copy($scope.model);

        $scope.kvdelimiter = $scope.config['kv-delimiter'] || ':';
        $scope.delimiter = $scope.config.delimiter || ',';

        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            //intialize to one empty property
            $scope.properties.push({
              key: '',
              value: ''
            });

            return;
          }
          var arr = str.split($scope.delimiter);

          angular.forEach(arr, function(a) {
            var split = a.split($scope.kvdelimiter);

            $scope.properties.push({
              key: split[0],
              value: split[1]
            });
          });
        }

        initialize();

        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);

          initialize();
        });

        $scope.$watch('properties', function() {

          var str = '';

          angular.forEach($scope.properties, function(p) {
            if (p.key.length > 0) {
              str = str + p.key + $scope.kvdelimiter + p.value + $scope.delimiter;
            }
          });

          // remove last delimiter
          if (str.length > 0 && str.charAt(str.length - 1) === $scope.delimiter ) {
            str = str.substring(0, str.length - 1);
          }

          $scope.model = str;

        }, true);


        $scope.addProperty = function() {
          $scope.properties.push({
            key: '',
            value: '',
            newField: 'add'
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);
        };

        $scope.enter = function (event, last) {
          if (last && event.keyCode === 13) {
            $scope.addProperty();
          }
        };
      }
    };
  });
