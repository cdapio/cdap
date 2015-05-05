angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    $scope.activeTab = 0;
    var flowletid = $scope.$parent.activeFlowlet.name;

    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {
        $scope.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description');

      });

    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + flowletid + '/instances'
      })
      .then(function (res){
        $scope.provisionedInstances = res.instances;
        $scope.instance = res.instances;
      });

    dataSrc
      .poll({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + flowletid + '/instances'
      }, function(res) {
        $scope.provisionedInstances = res.instances;
      });


    $scope.setInstance = function () {
      dataSrc
        .request({
          _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId + '/flowlets/' + flowletid + '/instances',
          method: 'PUT',
          body: {
            'instances': $scope.instance
          }
        });
    };

  });
