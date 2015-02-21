angular.module(PKG.name + '.commons')
  .directive('myProgramHistory', function() {
    return {
      restrict: 'EA',
      scope: {
        runs: '='
      },
      templateUrl: 'program-history/program-history.html',
      link: function(scope, element, attrs) {
        scope.runs = attrs.runs;
      }
    }
  });
