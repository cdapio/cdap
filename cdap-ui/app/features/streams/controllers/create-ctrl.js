angular.module(PKG.name + '.feature.streams')
  .controller('StreamsCreateController', function($scope) {
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
  });
