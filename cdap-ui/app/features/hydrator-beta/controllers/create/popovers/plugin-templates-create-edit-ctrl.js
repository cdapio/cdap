angular.module(`${PKG.name}.feature.hydrator-beta`)
  .controller('PluginTemplatesCreateEditCtrl', function ($scope, PluginTemplateStoreBeta, PluginTemplateActionBeta, PluginActionsFactoryBeta, $stateParams) {
      $scope.closeTemplateCreationModal = ()=> {
        PluginTemplateActionBeta.reset();
        $scope.$close();
      };
      $scope.saveAndClose = () => {
        PluginTemplateActionBeta.triggerSave();
      };
      $scope.pluginTemplateSaveError = null;
      PluginTemplateStoreBeta.registerOnChangeListener(() => {
        let getIsSaveSuccessfull = PluginTemplateStoreBeta.getIsSaveSuccessfull();
        if (getIsSaveSuccessfull) {
          PluginTemplateActionBeta.reset();
          PluginActionsFactoryBeta.fetchTemplates({namespace: $stateParams.namespace});
          $scope.$close();
        }
      });

  });
