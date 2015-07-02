angular.module(PKG.name + '.commons')
  .directive('mySidePanel', function () {
    return {
      restrict: 'E',
      scope: {
        panelGroups: '=',
        panelConfig: '='
      },
      templateUrl: 'side-panel/side-panel.html',
      controller: 'MySidePanel',
      controllerAs: 'MySidePanel'
    };
  });
