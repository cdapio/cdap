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

class HydratorPlusPlusPreConfiguredCtrl {
  constructor (rTemplateType, GLOBALS, myPipelineTemplatesApi, HydratorPlusPlusHydratorService, HydratorPlusPlusCanvasFactory, DAGPlusPlusNodesActionsFactory, $state) {
    this.currentPage = 1;
    this.templates = [];
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusCanvasFactory = HydratorPlusPlusCanvasFactory;
    this.myPipelineTemplatesApi = myPipelineTemplatesApi;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.$state = $state;

    this.typeFilter = (rTemplateType === GLOBALS.etlBatch? GLOBALS.etlBatch: GLOBALS.etlRealtime);
    this.fetchTemplates().then((plugins) => {
      this.templates = plugins;
    });
  }

  selectTemplate(template) {
    let result = this.HydratorPlusPlusCanvasFactory.parseImportedJson(
      JSON.stringify(template._properties),
      template.type
    );
    if (result.error) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Imported pre-defined app has issues. Please check the JSON of the imported pre-defined app.'
      });
    } else {
      this.$state.go('hydratorplusplus.create', {
        data: result
      });
    }
  }

  fetchTemplates() {
    return this.myPipelineTemplatesApi.list({
      apptype: this.typeFilter
    })
      .$promise
      .then( (res) => {
        let plugins = res.map( (plugin) => {
          return {
            name: plugin.name,
            description: plugin.description,
            type: this.typeFilter
          };
        });

        angular.forEach(plugins, (plugin) => {
          this.myPipelineTemplatesApi.get({
            apptype: this.typeFilter,
            appname: plugin.name
          })
            .$promise
            .then( (res) => {
              plugin._properties = res;
            });
        });

        return plugins;
      });
  }

}

HydratorPlusPlusPreConfiguredCtrl.$inject = ['rTemplateType', 'GLOBALS', 'myPipelineTemplatesApi', 'HydratorPlusPlusHydratorService', 'HydratorPlusPlusCanvasFactory', 'DAGPlusPlusNodesActionsFactory', '$state'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPreConfiguredCtrl', HydratorPlusPlusPreConfiguredCtrl);
