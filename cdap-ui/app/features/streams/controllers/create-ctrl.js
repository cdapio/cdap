angular.module(PKG.name + '.feature.streams')
  .controller('StreamsCreateController', function($scope, $filter, MyDataSource, $modalInstance) {

    var dataSrc = new MyDataSource($scope);

    var i = 0,
        filterFilter = $filter('filter');
    // $scope.type = 'Time Series';
    // $scope.id = 'Some Long string that is supposed to be ID';
    $scope.displayName = '';
    // $scope.properties = [
    //   {
    //     key: 'key1',
    //     value: 'value1'
    //   },
    //   {
    //     key: 'key2',
    //     value: 'value2'
    //   }
    // ];

    // $scope.addProperties = function() {
    //   $scope.properties.push({
    //     key: 'Property ' + i,
    //     value: ''
    //   });
    //   i+=1;
    // };

    // $scope.removeProperty = function(property) {
    //   var match = filterFilter($scope.properties, property);
    //   if (match.length) {
    //     $scope.properties.splice($scope.properties.indexOf(match[0]), 1);
    //   }
    // };



    $scope.createStream = function() {
      dataSrc
        .request({
          _cdapNsPath: '/streams/' + $scope.displayName,
          method: 'PUT'
        })
        .then(function(res) {
          console.log('res',res);
          $modalInstance.close(res);
        }, function(err) {
          console.log('err', err);
          $scope.error = err;
        });
    };

    $scope.closeModal = function() {
      $modalInstance.close();
    };

    $scope.emptyInput = function() {
      return $scope.displayName.length === 0;
    };

  });
