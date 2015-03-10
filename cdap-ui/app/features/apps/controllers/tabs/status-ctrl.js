angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppDetailStatusController', function($state, $scope, $stateParams, MyDataSource) {
    var basePath = '/apps/' + $stateParams.appId;

    $scope.programs = [];
    var datasrc = new MyDataSource($scope);
    var programTypes = [
          'flows',
          'services',
          'mapreduce',
          'workflows'
        ];
    datasrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        res.programs.forEach(function(prog) {
          prog.type_plural = prog.type +
            ((['Mapreduce', 'Spark'].indexOf(prog.type) === -1) ? 's': '');

          fetchStatus(prog.type_plural.toLowerCase(), prog.name);
        });
        $scope.programs = res.programs;
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
            return item.name === programId;
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
