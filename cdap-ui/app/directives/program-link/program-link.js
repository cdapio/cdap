angular.module(PKG.name + '.commons')
  .directive('myProgramLink', function() {
    return {
      restrict: 'E',
      scope: {
        type: '=',
        app: '=',
        program: '=',
        namespace: '='
      },
      templateUrl: 'program-link/program-link.html'
    };
  });
