angular.module(`${PKG.name}.feature.hydrator-beta`)
  .controller('PluginTemplatesDeleteCtrl', function(rNode, $scope, mySettings, $stateParams, myAlertOnValium, PluginActionsFactoryBeta) {
    let node = rNode;
    $scope.templateName = node.pluginTemplate;
    $scope.ok = () => {
      mySettings.get('pluginTemplates', true)
        .then( (res) => {
          delete res[$stateParams.namespace][node.templateType][node.pluginType][node.pluginTemplate];
          return mySettings.set('pluginTemplates', res);
        })
        .then( () => {
          myAlertOnValium.show({
            type: 'success',
            content: 'Successfully deleted template ' + node.pluginTemplate
          });
          PluginActionsFactoryBeta.fetchTemplates({namespace: $stateParams.namespace});
        });
      $scope.$close();
    };
  });
