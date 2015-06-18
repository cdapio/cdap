angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', function($state, $scope, myHelpers, myFlowsApi) {

    this.activeTab = 0;
    var flowletid = $scope.FlowletsController.activeFlowlet.name;

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {
        this.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description');
      }.bind(this));

    params.flowletId = flowletid;

    myFlowsApi.getFlowletInstance(params)
      .$promise
      .then(function (res){
        this.provisionedInstances = res.instances;
        this.instance = res.instances;
      }.bind(this));

    myFlowsApi.pollFlowletInstance(params)
      .$promise
      .then(function (res) {
        this.provisionedInstances = res.instances;
      }.bind(this));

    this.setInstance = function () {
      myFlowsApi.setFlowletInstance(params, { 'instances': this.instance });
    };

  });
