angular.module(PKG.name + '.feature.programs')
  .controller('ProgramsListController', function($scope, $stateParams, MyDataSource) {
    var datasrc = new MyDataSource($scope),
        basePath = '/apps/' + $stateParams.appId + '/' + $stateParams.programType;


    $scope.programs = [];
    datasrc.request(
      {
        _cdapNsPath: basePath
      },
      function(res) {
        $scope.programs = res;
        res.forEach(fetchStatus);
      }
    );

    // FIXME: Not DRY. Almost same thing done in CdapAppDetailStatusController
    function fetchStatus(program) {
      var programId = program.id;
      datasrc.poll(
        {
          _cdapNsPath: basePath + '/' +
                        programId + '/status'
        },
        function (res) {
          var program = $scope.programs.filter(function(item) {
            return item.id === programId;
          });
          program[0].status = res.status;
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
    }
  });
