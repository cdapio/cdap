angular.module(PKG.name + '.commons')
  .directive('myProgramStatus', function () {

    return {
      restrict: 'E',
      scope: {
        status: '=',
        start: '=',
        duration: '='
      },
      templateUrl: 'program-status/program-status.html'
    };
  });
