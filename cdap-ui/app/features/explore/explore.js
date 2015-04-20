angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, MyDataSource, $state) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;

    $scope.dataList = []; // combined datasets and streams

    dataSrc.request({
      _cdapNsPath: '/data/datasets'
    }).then(function(res) {
      $scope.dataList = $scope.dataList.concat(res);
      console.log('res', res);
      // initializing
      $scope.type = 'dataset';
      $scope.name = res[0].name;
    });

    dataSrc.request({
      _cdapNsPath: '/streams'
    }).then(function(res) {
      angular.forEach(res, function(s) {
        dataSrc.request({
          _cdapNsPath: '/streams/' + s.name
        }).then(function(properties) {
          s.properties = properties;
        });
      });

      $scope.dataList = $scope.dataList.concat(res);
    });

    $scope.click = function (data) {
      $scope.selected = data;
      console.log('data', JSON.parse(data.properties));


      if (data.type === 'Stream') {
        $scope.type = 'stream';
      } else {
        $scope.type = 'dataset';
      }
      $scope.name = data.name;

    };

  });
