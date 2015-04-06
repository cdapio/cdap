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

        // INPUTS
        angular.forEach(res.connections, function(v) {
          if (v.targetName === flowletid) {
            $scope.inputs.push(v.sourceName);
          }
        });

        // OUTPUTS
        angular.forEach(res.connections, function(v) {
          if (v.sourceName === flowletid) {
            $scope.outputs.push(v.targetName);
          }
        });

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


    // INPUT METRICS
    dataSrc
      .poll({
        _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
                      + '.app.' + $state.params.appId
                      + '.flow.' + $state.params.programId
                      + '.flowlet.' + 'source'
                      + '&metric=system.process.events.out&start=now-60s&count=60',
        method: 'POST'
      }, function (res) {
        updateInput(res.series[0].data);
      });

    function updateInput(newVal) {
      if(angular.isObject(newVal)) {
        var v = [];

        angular.forEach(newVal, function(val) {
          v.push({
            time: val.time,
            y: val.value
          });
        });

        if ($scope.inputHistory) {
          $scope.inputStream = v.slice(-1);
        }

        $scope.inputHistory = [
          {
            label: 'output',
            values: v
          }
        ];

      }
    }


    // OUTPUT METRICS
    // dataSrc
    //   .poll({
    //     _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
    //                   + '.app.' + $state.params.appId
    //                   + '.flow.' + $state.params.programId
    //                   + '.flowlet.' + $state.params.flowletid
    //                   + '&metric=system.process.events.out&start=now-60s&count=60',
    //     method: 'POST'
    //   }, function(res) {
    //     updateOutput(res.series[0].data);
    //   });

    // function updateOutput(newVal) {
    //   if(angular.isObject(newVal)) {
    //     var v = [];

    //     angular.forEach(newVal, function(val) {
    //       v.push({
    //         time: val.time,
    //         y: val.value
    //       });
    //     });

    //     if ($scope.outputHistory) {
    //       $scope.outputStream = v.slice(-1);
    //     }

    //     $scope.outputHistory = [
    //       {
    //         label: 'output',
    //         values: v
    //       }
    //     ];

    //   }
    // }





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
