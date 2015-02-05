angular.module(PKG.name + '.feature.flows')
  .controller('CdapflowsListController', function($scope, $stateParams, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $stateParams.appId + '/flows';
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        $scope.flows = res;
        res.forEach(pollStatus);
      });

    function pollStatus(program) {
      var programId = program.id;
      dataSrc.poll({
        _cdapNsPath: basePath + '/' +
                     programId + '/status'
      }, function (res) {
        program.status = res.status;
      });
    }

    $scope.doProgram = function(programId, action) {
      var matchedFlow = $scope.flows.filter(function(flow) {
        return flow.id === programId;
      });
      matchedFlow[0].status = 'UPDATING';
      dataSrc.request({
          _cdapNsPath: basePath + '/' + programId + '/' + action,
          method: 'POST'
      });
    };
  });
