/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
  .controller('HydratorCreateController', function($timeout, $state, GLOBALS, myAlertOnValium, NonStorePipelineErrorFactory) {

    var vm = this;
    vm.GLOBALS = GLOBALS;
    vm.currentPage = 1;

    vm.preconfigured = false;
    vm.templates = [];
    vm.GLOBALS = GLOBALS;


    this.importFile = function(files) {
      var reader = new FileReader();
      reader.readAsText(files[0], 'UTF-8');

      reader.onload = function (evt) {
        var data = evt.target.result;
        var jsonData;
        try {
          jsonData = JSON.parse(data);
        } catch(e) {
          myAlertOnValium.show({
            type: 'danger',
            content: 'Syntax Error. Ill-formed pipeline configuration.'
          });
          return;
        }
        let isNotValid = NonStorePipelineErrorFactory.validateImportJSON(jsonData);
        if (isNotValid) {
          myAlertOnValium.show({
            type: 'danger',
            content: isNotValid
          });
        } else {
          if (!jsonData.config.connections) {
            jsonData.config.connections = generateLinearConnections(jsonData.config);
          }
          $state.go('hydrator.create.studio', {
            data: jsonData,
            type: jsonData.artifact.name
          });
        }
      };
    };

    let generateLinearConnections = (config) => {
      let nodes = [config.source].concat(config.transforms || []).concat(config.sinks);
      let connections = [];
      let i;
      for (i=0; i<nodes.length - 1 ; i++) {
        connections.push({ from: nodes[i].name, to: nodes[i+1].name });
      }
      return connections;
    };

    this.openFileBrowser = function() {
      $timeout(function() {
        document.getElementById('pipeline-import-config-link').click();
      });
    };

  });
