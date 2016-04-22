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
  constructor (rTemplateType, GLOBALS, myPipelineTemplatesApi, HydratorPlusPlusHydratorService, HydratorPlusPlusCanvasFactory, DAGPlusPlusNodesActionsFactory, $state, HydratorPlusPlusConfigStore, myAlertOnValium) {
    this.currentPage = 1;
    this.templates = [];
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusCanvasFactory = HydratorPlusPlusCanvasFactory;
    this.myPipelineTemplatesApi = myPipelineTemplatesApi;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.GLOBALS = GLOBALS;
    this.$state = $state;
    this.myAlertOnValium = myAlertOnValium;

    this.typeFilter = rTemplateType;
    this.fetchTemplates().then((plugins) => {
      this.templates = plugins;
    });
  }

  selectTemplate(template) {
    this.HydratorPlusPlusConfigStore.setState(this.HydratorPlusPlusConfigStore.getDefaults());
    this.$state.go('hydratorplusplus.create', {
      data: template._properties
    });
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
              delete plugin._properties.$promise;
              delete plugin._properties.$resolved;

              plugin._source = res.config.stages.filter( (stage) => {
                return this.GLOBALS.pluginConvert[stage.plugin.type] === 'source';
              });
              plugin._sinks = res.config.stages.filter( (stage) => {
                return this.GLOBALS.pluginConvert[stage.plugin.type] === 'sink';
              });
            });
        });

        return plugins;
      });
  }

}

HydratorPlusPlusPreConfiguredCtrl.$inject = ['rTemplateType', 'GLOBALS', 'myPipelineTemplatesApi', 'HydratorPlusPlusHydratorService', 'HydratorPlusPlusCanvasFactory', 'DAGPlusPlusNodesActionsFactory', '$state', 'HydratorPlusPlusConfigStore', 'myAlertOnValium'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPreConfiguredCtrl', HydratorPlusPlusPreConfiguredCtrl);
