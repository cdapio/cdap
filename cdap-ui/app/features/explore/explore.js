angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, MyDataSource, $state, myHelpers) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;
    $scope.openGeneral = false;
    $scope.openSchema = false;
    $scope.openPartition = false;

    $scope.dataList = []; // combined datasets and streams

    dataSrc.request({
      _cdapNsPath: '/data/explore/tables'
    }).then(function(res) {
      $scope.dataList = res;
      $scope.click(res[0]);
    });


    $scope.click = function (data) {

      dataSrc.request({
        _cdapNsPath: '/data/explore/tables/' + data.table + '/info'
      }).then(function (res) {
        $scope.selectedInfo = res;
        console.log('RES', res);
      });

    };

  });
