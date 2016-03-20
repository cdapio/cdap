angular.module(`${PKG.name}.feature.hydrator-beta`)
  .controller('PluginTemplatesCreateEditCtrl', function ($scope, PluginTemplateStoreBeta, PluginTemplateActionBeta, PluginActionsFactoryBeta, $stateParams, myAlertOnValium) {
      $scope.closeTemplateCreationModal = ()=> {
        PluginTemplateActionBeta.reset();
        $scope.$close();
      };
      $scope.pluginTemplateSaveError = null;
      PluginTemplateStoreBeta.registerOnChangeListener(() => {
        let getIsSaveSuccessfull = PluginTemplateStoreBeta.getIsSaveSuccessfull();
        let getIsCloseCommand = PluginTemplateStoreBeta.getIsCloseCommand();
        if (getIsSaveSuccessfull) {
          PluginTemplateActionBeta.reset();
          PluginActionsFactoryBeta.fetchTemplates({namespace: $stateParams.namespace});
          myAlertOnValium.show({
            type: 'success',
            content: 'Plugin template creation successfull'
          });
          $scope.$close();
        }
        if (getIsCloseCommand) {
          PluginTemplateActionBeta.reset();
          $scope.$close();
        }
      });

  });
