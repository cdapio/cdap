angular.module(PKG.name + '.commons')
  .directive('myRulesContainer', function() {
    return {
      restrict: 'E',
      scope: {
        fieldObj: '=',
        onDelete: '&',
        onDeleteContext: '='
      },
      templateUrl: 'rules-container/my-rules-container.html',
      controller: 'MyRulesContainerCtrl',
      controllerAs: 'MyRulesContainerCtrl'
    };
  });
