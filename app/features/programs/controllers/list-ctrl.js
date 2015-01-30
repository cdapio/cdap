angular.module(PKG.name + '.feature.programs')
  .controller('ProgramsListController', function($scope, $state, $stateParams, MyDataSource) {
    var datasrc = new MyDataSource($scope),
        basePath = '/apps/' + $stateParams.appId + '/' + $stateParams.programType;


    $scope.programs = [];
    datasrc.request(
      {
        _cdapNsPath: basePath
      },
      function(res) {
        $scope.programs = res;
        res.forEach(pollStatus);
      }
    );

    // FIXME: Not DRY. Almost same thing done in CdapAppDetailStatusController
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

    $scope.goToDetail = function(programId) {
      $state.go($stateParams.programType + '.detail', {
        programId: programId
      });
    };

    $scope.doProgram = function(programId, action) {
      datasrc.request(
        {
          _cdapNsPath: basePath + '/' + programId + '/' + action,
          method: 'POST'
        }
      );
    };
  });
