angular.module(PKG.name + '.feature.cdap-app')
  .controller('CdapAppDetailStatusController', function(MyDataSource, $scope, $stateParams) {
    var programTypes = [
      'flows',
      'services',
      'mapreduce',
      'procedures',
      'spark'
    ];
    $scope.programs = [];
    var datasrc = new MyDataSource($scope);
    programTypes.forEach(function(program, index) {
      datasrc.request(
        {
          _cdapNsPath: '/apps/' + $stateParams.appId + '/' + program
        },
        function(res) {
          $scope.programs = $scope.programs.concat(res);
          res.forEach(function(prog, index) {
            fetchStatus(program, prog.id);
          });
        }
      );
    });

    function fetchStatus(program, programId) {
      datasrc.request(
        {
          _cdapNsPath: '/apps/' + $stateParams.appId +
                        '/' + program + '/' +
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
