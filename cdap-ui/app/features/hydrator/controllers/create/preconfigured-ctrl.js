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

class HydratorPreconfiguredController {
  constructor(myPipelineTemplatesApi, GLOBALS, CanvasFactory, myAlertOnValium, $state) {
    this.CanvasFactory = CanvasFactory;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.GLOBALS = GLOBALS;

    this.templates = [];
    this.currentPage = 1;
    this.typeFilter = '';

    myPipelineTemplatesApi.list({
      apptype: this.GLOBALS.etlBatch
    })
      .$promise
      .then( (res) => {
        let plugins = res.map( (plugin) => {
          return {
            name: plugin.name,
            description: plugin.description,
            type: this.GLOBALS.etlBatch
          };
        });

        angular.forEach(plugins, (plugin) => {
          myPipelineTemplatesApi.get({
            apptype: this.GLOBALS.etlBatch,
            appname: plugin.name
          })
            .$promise
            .then( (res) => {
              plugin._properties = res;
            });
        });

        this.templates = this.templates.concat(plugins);
      });

    myPipelineTemplatesApi.list({
      apptype: this.GLOBALS.etlRealtime
    })
      .$promise
      .then( (res) => {
        let plugins = res.map( (plugin) => {
          return {
            name: plugin.name,
            label: plugin.name,
            description: plugin.description,
            type: this.GLOBALS.etlRealtime
          };
        });

        angular.forEach(plugins, (plugin) => {
          myPipelineTemplatesApi.get({
            apptype: this.GLOBALS.etlRealtime,
            appname: plugin.name
          })
            .$promise
            .then( (res) => {
              plugin._properties = res;
            });
        });

        this.templates = this.templates.concat(plugins);
      });
  }

  selectTemplate (template) {
    let result = this.CanvasFactory.parseImportedJson(
      JSON.stringify(template._properties),
      template.type
    );
    if (result.error) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Imported pre-defined app has issues. Please check the JSON of the imported pre-defined app.'
      });
    } else {
      this.$state.go('hydrator.create.studio', {
        data: result,
        type: result.artifact.name
      });
    }
  }
}


HydratorPreconfiguredController.$inject = ['myPipelineTemplatesApi', 'GLOBALS', 'CanvasFactory', 'myAlertOnValium', '$state'];

angular.module(PKG.name + '.feature.hydrator')
 .controller('HydratorPreconfiguredController', HydratorPreconfiguredController);
