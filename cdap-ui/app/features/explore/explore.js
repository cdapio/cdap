angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, $state, EventPipe, myExploreApi) {

    $scope.activeTab = 0;

    $scope.activePanel = [0];
    $scope.openGeneral = true;
    $scope.openSchema = false;
    $scope.openPartition = false;

    $scope.dataList = []; // combined datasets and streams

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myExploreApi.list(params)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(v) {
          var split = v.table.split('_');
          v.type = split[0];
          v.name = split[1];
        });

        $scope.dataList = res;
        $scope.selectTable(res[0]);
      });

    EventPipe.on('explore.newQuery', function() {
      if ($scope.activePanel.indexOf(1) === -1) {
        $scope.activePanel = [0,1];
      }
    });

    $scope.selectTable = function (data) {
      // Passing this info to sql-query directive
      $scope.type = data.type;
      $scope.name = data.name;

      params.table = data.table;

      // Fetching info of the table
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          $scope.selectedInfo = res;
        });

    };

  });
