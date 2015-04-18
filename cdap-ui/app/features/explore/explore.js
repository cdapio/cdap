angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, MyDataSource, $state) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;

    $scope.dataList = []; // combined datasets and streams

    dataSrc.request({
      _cdapNsPath: '/data/datasets'
    }).then(function(res) {
      $scope.dataList = $scope.dataList.concat(res);
    });

    dataSrc.request({
      _cdapNsPath: '/streams'
    }).then(function(res) {
      $scope.dataList = $scope.dataList.concat(res);
    });

    $scope.click = function (data) {

      if (data.type === 'Stream') {
        $scope.type = 'stream';
      } else {
        $scope.type = 'dataset';
      }
      $scope.name = data.name;

    };

  });
