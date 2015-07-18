angular.module(PKG.name + '.commons')
  .directive('myRule', function() {
    return {
      restrict: 'E',
      scope: {
        rule: '=',
        type: '='
      },
      templateUrl: 'rule/my-rule.html',
      controller: 'MyRuleCtrl',
      controllerAs: 'MyRuleCtrl'
    }
  });
