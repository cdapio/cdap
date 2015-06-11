angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', function($state, $scope, myHelpers, myFlowsApi) {

    $scope.activeTab = 0;
    var flowletid = $scope.RunsController.activeFlowlet.name;

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {
        $scope.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description');

      });

    params.flowletId = flowletid;

    myFlowsApi.getFlowletInstance(params)
      .$promise
      .then(function (res){
        $scope.provisionedInstances = res.instances;
        $scope.instance = res.instances;
      });

    myFlowsApi.pollFlowletInstance(params)
      .$promise
      .then(function (res) {
        $scope.provisionedInstances = res.instances;
      });

    $scope.setInstance = function () {
      myFlowsApi.setFlowletInstance(params, { 'instances': $scope.instance });
    };

  });
