angular.module(`${PKG.name}.commons`)
  .directive('pluginTemplates', function() {
    return {
      restrict: 'EA',
      scope: {},
      templateUrl: 'plugin-templates/plugin-templates.html',
      controller: 'PluginTemplatesCtrl',
      controllerAs: 'PluginTemplatesCtrl'
    };
  });
