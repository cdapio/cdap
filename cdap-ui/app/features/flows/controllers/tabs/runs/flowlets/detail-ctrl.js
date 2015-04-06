angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    $scope.activeTab = 0;
    var flowletid = $state.params.flowletid;
    $scope.inputs = [];
    $scope.outputs = [];
    $scope.datasets = [];

    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {
        $scope.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description');

        $scope.datasets = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'dataSets');

      });

    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + $state.params.flowletid + '/instances'
      })
      .then(function (res){
        $scope.provisionedInstances = res.instances;
        $scope.instance = res.instances;
      });

    dataSrc
      .poll({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + $state.params.flowletid + '/instances'
      }, function(res) {
        $scope.provisionedInstances = res.instances;
      });


    $scope.setInstance = function () {
      dataSrc
        .request({
          _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + $state.params.flowletid + '/instances',
          method: 'PUT',
          body: {
            'instances': $scope.instance
          }
        });
    };

  });
