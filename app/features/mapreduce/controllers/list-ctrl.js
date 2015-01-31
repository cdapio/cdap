angular.module(PKG.name + '.feature.mapreduce')
  .controller('CdapMapreduceListController', function($scope, $state, $stateParams, MyDataSource) {
    var datasrc = new MyDataSource($scope),
        basePath = '/apps/' + $stateParams.appId + '/mapreduce';


    $scope.mapreduceList = [];
    datasrc.request(
      {
        _cdapNsPath: basePath
      },
      function(res) {
        $scope.mapreduceList = res;
        res.forEach(pollStatus);
      }
    );

    function pollStatus(program) {
      var programId = program.id;
      datasrc.poll(
        {
          _cdapNsPath: basePath + '/' +
                        programId + '/status'
        },
        function (res) {
          program.status = res.status;
        }
      );
    }

    $scope.doProgram = function(programId, action) {
      datasrc.request(
        {
          _cdapNsPath: basePath + '/' + programId + '/' + action,
          method: 'POST'
        }
      );
    };
  });
