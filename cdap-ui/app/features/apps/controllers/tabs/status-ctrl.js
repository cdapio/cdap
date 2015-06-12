angular.module(PKG.name + '.feature.apps')
  .controller('AppDetailStatusController', function($state, $scope, $stateParams, MyDataSource) {
    var basePath = '/apps/' + $stateParams.appId;

    this.programs = [];
    var datasrc = new MyDataSource($scope);
    datasrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        res.programs.forEach(function(prog) {
          prog.type_plural = prog.type +
            ((['Mapreduce', 'Spark'].indexOf(prog.type) === -1) ? 's': '');

          fetchStatus.bind(this)(prog.type_plural.toLowerCase(), prog.name);
        }.bind(this));

        this.programs = res.programs;
      }.bind(this));

    // FIXME: Not DRY. Almost same thing done in ProgramsListController
    function fetchStatus(program, programId) {
      datasrc.poll(
        {
          _cdapNsPath: basePath + '/' + program + '/' +
                        programId + '/status'
        },
        function (res) {
          var program = this.programs.filter(function(item) {
            return item.name === programId;
          }.bind(this));
          program[0].status = res.status;
        }.bind(this)
      );
    }

    this.goToDetail = function(programType, program) {
      $state.go(programType.toLowerCase() + '.detail', {
        programId: program
      });
    };

    //ui-sref="programs.type({programType: (program.type_plural | lowercase)})"
    this.goToList = function(programType) {
      $state.go(programType.toLowerCase() + '.list');
    };

  });
