angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunsDetailStatusController', function($state, $scope, MyDataSource, $filter) {
    var filterFilter = $filter('filter');

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    $scope.endPoints = [];

    $scope.basePath = '/namespaces/' + $state.params.namespace + path;

    dataSrc.request({
      _cdapNsPath: path
    })
      .then(function(res) {
        angular.forEach(res.handlers, function(value) {
          $scope.endPoints = $scope.endPoints.concat(value.endpoints);
        });
      });

    // disable make request button on inactive runs
    dataSrc.request({
      _cdapNsPath: path + '/runs/' + $scope.runs.selected.runid
    })
    .then(function(res) {
      $scope.status = res.status;
    });

  });
