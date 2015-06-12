angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunsDetailStatusController', function($state, $scope, $filter, myServiceApi) {
    var filterFilter = $filter('filter');

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

    var path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    $scope.endPoints = [];

    $scope.basePath = '/namespaces/' + $state.params.namespace + path;

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      serviceId: $state.params.programId,
      scope: $scope
    };

    myServiceApi.get(params)
      .$promise
      .then(function(res) {
        angular.forEach(res.handlers, function(value) {
          $scope.endPoints = $scope.endPoints.concat(value.endpoints);
        });
      });

    params.runId = $scope.runs.selected.runid;

    myServiceApi.runDetail(params)
      .$promise
      .then(function(res) {
        $scope.status = res.status;
      });

  });
