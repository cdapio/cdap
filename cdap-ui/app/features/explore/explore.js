angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, MyDataSource, $state, myHelpers) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;
    $scope.openGeneral = true;
    $scope.openSchema = false;
    $scope.openPartition = false;

    $scope.dataList = []; // combined datasets and streams

    dataSrc.request({
      _cdapNsPath: '/data/explore/tables'
    }).then(function(res) {
      angular.forEach(res, function(v) {
        var split = v.table.split('_');
        v.type = split[0];
        v.name = split[1];
      });

      $scope.dataList = res;
      $scope.selectTable(res[0]);
    });


    $scope.selectTable = function (data) {
      $scope.type = data.type;
      $scope.name = data.name;

      dataSrc.request({
        _cdapNsPath: '/data/explore/tables/' + data.table + '/info'
      }).then(function (res) {
        $scope.selectedInfo = res;
      });

    };

  });
