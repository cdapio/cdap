/*
  Copyright © 2015 Cask Data, Inc.

  Licensed under the Apache License, Version 2.0 (the "License"); you may not
  use this file except in compliance with the License. You may obtain a copy of
  the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations under
  the License.
*/

class AppDetailStatusController {
  constructor($state, myPipelineApi, MyAppDAGService, CanvasFactory, GLOBALS, $scope) {
    this.nodes = [];
    var params = {
      namespace: $state.params.namespace,
      pipeline: $state.params.appId,
      scope: $scope
    };

    myPipelineApi.get(params)
      .$promise
      .then( (res)=> {
        try{
          res.config = JSON.parse(res.configuration);
        } catch(e) {
          console.log('ERROR in configuration from backend: ', e);
        }
        this.config = {
          name: $state.params.appId,
          artifact: res.artifact,
          description: res.description,
          config: {
            source: res.config.source,
            sinks: res.config.sinks,
            transforms: res.config.transforms,
            instances: res.instance,
            schedule: res.config.schedule
          }
        };

        MyAppDAGService.metadata.name = res.name;
        MyAppDAGService.metadata.description = res.description;
        MyAppDAGService.metadata.template.type = res.artifact.name;
        if (res.artifact.name === GLOBALS.etlBatch) {
          MyAppDAGService.metadata.template.schedule = res.config.schedule;
        } else if (res.artifact.name === GLOBALS.etlRealtime) {
          MyAppDAGService.metadata.template.instances = res.config.instances;
        }
        this.nodes = CanvasFactory.getNodes(res.config, MyAppDAGService.metadata.template.type);
        this.nodes.forEach( (node)=> { MyAppDAGService.addNodes(node, node.type); });

        MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes(this.nodes, res.artifact.name);
      },
      () => {
        $state.go('404');
      }
    );
  }
}

AppDetailStatusController.$inject = ['$state', 'myPipelineApi', 'MyAppDAGService', 'CanvasFactory', 'GLOBALS', '$scope'];

angular.module(PKG.name + '.feature.apps')
  .controller('AppDetailStatusController', AppDetailStatusController);
