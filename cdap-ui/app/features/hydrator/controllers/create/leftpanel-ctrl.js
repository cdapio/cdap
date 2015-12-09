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

class LeftPanelController {
  constructor($scope, $stateParams, rVersion, GLOBALS, LeftPanelStore, LeftPanelActionsFactory, PluginActionsFactory, ConfigStore, ConfigActionsFactory, MyDAGFactory, NodesActionsFactory, HydratorErrorFactory, HydratorService) {
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.LeftPanelStore = LeftPanelStore;
    this.LeftPanelActionsFactory = LeftPanelActionsFactory;
    this.PluginActionsFactory = PluginActionsFactory;
    this.ConfigActionsFactory = ConfigActionsFactory;
    this.GLOBALS = GLOBALS;
    this.MyDAGFactory = MyDAGFactory;
    this.NodesActionsFactory = NodesActionsFactory;
    this.HydratorErrorFactory = HydratorErrorFactory;
    this.HydratorService = HydratorService;

    this.pluginTypes = [
      {
        name: 'source',
        expanded: false,
        plugins: []
      },
      {
        name: 'transform',
        expanded: false,
        plugins: []
      },
      {
        name: 'sink',
        expanded: false,
        plugins: []
      }
    ];
    this.sourcesToVersionMap = {};
    this.transformsToVersionMap = {};
    this.sinksToVersionMap = {};

    this.LeftPanelStore.registerOnChangeListener(() => {
      this.pluginTypes[0].plugins = this.LeftPanelStore.getSources();
      this.pluginTypes[1].plugins = this.LeftPanelStore.getTransforms();
      this.pluginTypes[2].plugins = this.LeftPanelStore.getSinks();
    });

    let params = {
      namespace: this.$stateParams.namespace,
      pipelineType: ConfigStore.getArtifact().name,
      version: rVersion.version,
      scope: this.$scope
    };
    this.PluginActionsFactory.fetchSources(params);
    this.PluginActionsFactory.fetchTransforms(params);
    this.PluginActionsFactory.fetchSinks(params);

  }

  onLeftSidePanelItemClicked(event, item) {
    event.stopPropagation();
    var plugin = this.LeftPanelStore.getSpecificPluginVersion(item);
    let config;

    if (plugin.pluginTemplate) {
      config = {
        label: plugin.name,
        name: plugin.pluginName,
        artifact: plugin.artifact,
        icon: this.MyDAGFactory.getIcon(plugin.pluginName),
        type: plugin.pluginType,
        properties: plugin.properties,
        outputSchema: plugin.outputSchema,
        pluginTemplate: plugin.pluginTemplate,
        lock: plugin.lock
      };
    } else {
      config = {
        label: plugin.name,
        artifact: plugin.artifact,
        name: plugin.name,
        icon: plugin.icon,
        description: plugin.description,
        type: plugin.type,
        properties: {},
        warning: true
      };
    }

    // set initial position
    switch (this.GLOBALS.pluginConvert[config.type]) {
      case 'source':
        config._uiPosition = {
          top: '150px',
          left: '10vw'
        };
        break;
      case 'transform':
        config._uiPosition = {
          top: '150px',
          left: '30vw'
        };
        break;
      case 'sink':
        config._uiPosition = {
          top: '150px',
          left: '50vw'
        };
        break;
    }

    // this.ConfigActionsFactory.addPlugin(config, this.GLOBALS.pluginConvert[config.type]);
    this.HydratorService.fetchBackendProperties(config)
      .then( () => {
        config.requiredFieldCount = this.HydratorErrorFactory.countRequiredFields(config);
        if (config.requiredFieldCount > 0) {
          config.error = {
            message: this.GLOBALS.en.hydrator.studio.genericMissingRequiredFieldsError
          };
        }
        this.NodesActionsFactory.addNode(config);
      } );

  }
}

LeftPanelController.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'LeftPanelStore', 'LeftPanelActionsFactory', 'PluginActionsFactory', 'ConfigStore', 'ConfigActionsFactory', 'MyDAGFactory', 'NodesActionsFactory', 'HydratorErrorFactory', 'HydratorService'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('LeftPanelController', LeftPanelController);
