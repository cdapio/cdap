angular.module(PKG.name + '.feature.streams')
  .controller('StreamsCreateController', function($scope, $filter) {
    var i = 0,
        filterFilter = $filter('filter');
    $scope.type = 'Time Series';
    $scope.id = 'Some Long string that is supposed to be ID';
    $scope.displayName = 'Stream1DisplayName';
    $scope.description = '';
    $scope.properties = [
      {
        key: 'key1',
        value: 'value1'
      },
      {
        key: 'key2',
        value: 'value2'
      }
    ];

    $scope.addProperties = function() {
      $scope.properties.push({
        key: 'Property ' + i,
        value: ''
      });
      i+=1;
    };

    $scope.removeProperty = function(property) {
      var match = filterFilter($scope.properties, property);
      if (match.length) {
        $scope.properties.splice($scope.properties.indexOf(match[0]), 1);
      }
    };
  });
