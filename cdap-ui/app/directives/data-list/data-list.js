angular.module(PKG.name+'.commons')
.directive('myDataList', function myDataListDirective () {
  return {
    restrict: 'E',
    scope: {
      level: '@',
      program: '@'
    },
    templateUrl: 'data-list/data-list.html',
    controller: function($scope, MyDataSource, $state) {
      var dataSrc = new MyDataSource($scope);

      var path = '/apps/' + $state.params.appId;

      if ($scope.level !== 'app') {
        switch ($scope.program) {
          case 'mapreduce':
            path += '/mapreduce/';
            break;
          case 'flow':
            path += '/flows/';
            break;
          case 'service':
            path += '/services/';
            break;
        }

        path += $state.params.programId;
      }

      $scope.dataList = [];

      // get datasets
      dataSrc.request({
        _cdapNsPath: path + '/datasets'
      }).then(function(res) {
        angular.forEach(res, function(d) {
          $scope.dataList.push({
            name: d.instanceId,
            type: 'Dataset'
          });
        });
      });

      // get streams
      dataSrc.request({
        _cdapNsPath: path + '/streams'
      }).then(function(res) {
        angular.forEach(res, function(s) {
          $scope.dataList.push({
            name: s.streamName,
            type: 'Stream'
          });
        });
      });

    }
  };

});
