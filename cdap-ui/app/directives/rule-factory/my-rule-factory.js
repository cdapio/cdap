angular.module(PKG.name + '.commons')
  .directive('myRuleFactory', function() {
    return {
      restrict: 'E',
      scope: {
        inputFields: '='
      },
      templateUrl: 'rule-factory/my-rule-factory.html',
      controller: 'MyRuleFactoryCtrl',
      controllerAs: 'MyRuleFactoryCtrl'
    };
  });
