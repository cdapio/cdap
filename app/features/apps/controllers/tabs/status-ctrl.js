angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppDetailStatusController', function($state, $scope, $stateParams, MyDataSource) {
    var programTypes = [
          'flows',
          'services',
          'mapreduce',
          'procedures',
          'spark',
          'workflows'
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
          res.forEach(function(program) {
            program.type_plural = program.type + ((['Mapreduce', 'Spark'].indexOf(program.type) === -1) ? 's': '');
          });
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

    $scope.goToDetail = function(programType, program) {
      $state.go(programType.toLowerCase() + '.detail', {
        programId: program
      });
    };

    //ui-sref="programs.type({programType: (program.type_plural | lowercase)})"
    $scope.goToList = function(programType) {
      $state.go(programType.toLowerCase() + '.list');
    };

  });
