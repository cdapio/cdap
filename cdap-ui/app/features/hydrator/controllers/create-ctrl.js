/*
 * Copyright © 2015 Cask Data, Inc.
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


angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateController', function($timeout, $state, $alert, myPipelineTemplatesApi, GLOBALS, CanvasFactory) {

    var vm = this;
    vm.GLOBALS = GLOBALS;
    vm.currentPage = 1;

    vm.preconfigured = false;
    vm.templates = [];
    vm.GLOBALS = GLOBALS;

    vm.typeFilter = '';


    myPipelineTemplatesApi.list({
      apptype: GLOBALS.etlBatch
    })
      .$promise
      .then(function(res) {
        var plugins = res.map(function(plugin) {
          return {
            name: plugin.name,
            description: plugin.description,
            type: GLOBALS.etlBatch
          };
        });

        angular.forEach(plugins, function (plugin) {
          myPipelineTemplatesApi.get({
            apptype: GLOBALS.etlBatch,
            appname: plugin.name
          })
            .$promise
            .then(function (res) {
              plugin._properties = res;
            });
        });

        vm.templates = vm.templates.concat(plugins);
      });

    myPipelineTemplatesApi.list({
      apptype: GLOBALS.etlRealtime
    })
      .$promise
      .then(function(res) {
        var plugins = res.map(function(plugin) {
          return {
            name: plugin.name,
            label: plugin.name,
            description: plugin.description,
            type: GLOBALS.etlRealtime
          };
        });

        angular.forEach(plugins, function (plugin) {
          myPipelineTemplatesApi.get({
            apptype: GLOBALS.etlRealtime,
            appname: plugin.name
          })
            .$promise
            .then(function (res) {
              plugin._properties = res;
            });
        });

        vm.templates = vm.templates.concat(plugins);
      });

    vm.selectTemplate = function (template) {
      var result = CanvasFactory.parseImportedJson(
        JSON.stringify(template._properties),
        template.type
      );
      if (result.error) {
        $alert({
          type: 'danger',
          content: 'Imported pre-defined app has issues. Please check the JSON of the imported pre-defined app'
        });
      } else {
        $state.go('hydrator.create.studio', {
          data: result,
          type: result.artifact.name
        }).then(function () {
          vm.preconfigured = false;
        });
      }
    };

    this.importFile = function(files) {
      var reader = new FileReader();
      reader.readAsText(files[0], 'UTF-8');

      reader.onload = function (evt) {
         var data = evt.target.result;
         var jsonData;
         try {
           jsonData = JSON.parse(data);
         } catch(e) {
           $alert({
             type: 'danger',
             content: 'Error in the JSON imported.'
           });
           console.log('ERROR in imported json: ', e);
           return;
         }
         $state.go('hydrator.create.studio', {
           data: jsonData,
           type: jsonData.artifact.name
         });
      };
    };

    this.openFileBrowser = function() {
      $timeout(function() {
        document.getElementById('pipeline-import-config-link').click();
      });
    };

  });
