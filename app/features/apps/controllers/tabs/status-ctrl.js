angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppDetailStatusController', function($scope, $stateParams, MyDataSource) {
    var programTypes = [
          'flows',
          'services',
          'mapreduce',
          'procedures',
          'spark'
        ],
        basePath = '/apps/' + $stateParams.appId;

    $scope.programs = [];
    var datasrc = new MyDataSource($scope);

    programTypes.forEach(function(program) {
      datasrc.request(
        {
          _cdapNsPath: basePath + '/' + program
        },
        function(res) {
          $scope.programs = $scope.programs.concat(res);
          res.forEach(function(prog) {
            fetchStatus(program, prog.id);
          });
        }
      );
    });

    // FIXME: Not DRY. Almost same thing done in ProgramsListController
    function fetchStatus(program, programId) {
      datasrc.poll(
        {
          _cdapNsPath: basePath + '/' + program + '/' +
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

  });
