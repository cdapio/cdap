/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('PluginTemplatesDeleteCtrl', function(rNode, $scope, mySettings, $stateParams, myAlertOnValium, HydratorPlusPlusPluginActions, HydratorPlusPlusLeftPanelStore, rTemplateType) {
    let node = rNode;
    $scope.templateName = node.pluginTemplate;
    $scope.ok = () => {
      $scope.disableOKButton = true;
      mySettings.get('pluginTemplates', true)
        .then( (res) => {
          delete res[$stateParams.namespace][node.templateType][node.pluginType][node.pluginTemplate];
          return mySettings.set('pluginTemplates', res);
        })
        .then(
          () => {
            $scope.disableOKButton = false;
            myAlertOnValium.show({
              type: 'success',
              content: 'Successfully deleted template ' + node.pluginTemplate
            });
            HydratorPlusPlusLeftPanelStore.dispatch(
              HydratorPlusPlusPluginActions.fetchTemplates(
                { namespace: $stateParams.namespace },
                { namespace: $stateParams.namespace, pipelineType: rTemplateType }
              )
            );

            $scope.$close();
          },
          (err) => {
            $scope.disableButtons = false;
            $scope.error = err;
          }
        );
    };
  });
