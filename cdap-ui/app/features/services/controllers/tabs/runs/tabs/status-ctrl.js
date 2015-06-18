angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunsDetailStatusController', function($state, $scope, $filter, myServiceApi) {
    var filterFilter = $filter('filter');

    if ($state.params.runid) {
      var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.RunsController.runs.selected = match[0];
      }
    }

    var path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    this.endPoints = [];

    this.basePath = '/namespaces/' + $state.params.namespace + path;

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
          this.endPoints = this.endPoints.concat(value.endpoints);
        }, this);
      }.bind(this));

    if ($scope.RunsController.runs.length > 0) {
      params.runId = $scope.RunsController.runs.selected.runid;

      myServiceApi.runDetail(params)
        .$promise
        .then(function(res) {
          this.status = res.status;
        }.bind(this));
    }

  });
