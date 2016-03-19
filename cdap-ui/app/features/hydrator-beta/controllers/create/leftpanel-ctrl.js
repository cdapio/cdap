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

class LeftPanelControllerBeta {
  constructor($scope, $stateParams, rVersion, GLOBALS, LeftPanelStoreBeta, LeftPanelActionsFactoryBeta, PluginActionsFactoryBeta, ConfigStoreBeta, ConfigActionsFactoryBeta, MyDAGFactoryBeta, NodesActionsFactoryBeta, NonStorePipelineErrorFactory, HydratorServiceBeta, $rootScope, $uibModal, $timeout, myAlertOnValium, $state, PluginTemplateActionBeta, mySettings) {
    this.$state = $state;
    this.$rootScope = $rootScope;
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.LeftPanelStoreBeta = LeftPanelStoreBeta;
    this.LeftPanelActionsFactoryBeta = LeftPanelActionsFactoryBeta;
    this.PluginActionsFactoryBeta = PluginActionsFactoryBeta;
    this.ConfigActionsFactoryBeta = ConfigActionsFactoryBeta;
    this.GLOBALS = GLOBALS;
    this.ConfigStoreBeta = ConfigStoreBeta;
    this.MyDAGFactoryBeta = MyDAGFactoryBeta;
    this.NodesActionsFactoryBeta = NodesActionsFactoryBeta;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.HydratorServiceBeta = HydratorServiceBeta;
    this.PluginTemplateActionBeta = PluginTemplateActionBeta;
    this.rVersion = rVersion;
    this.$timeout = $timeout;
    this.myAlertOnValium = myAlertOnValium;
    this.mySettings = mySettings;

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

    this.LeftPanelStoreBeta.registerOnChangeListener(() => {
      this.artifacts = this.LeftPanelStoreBeta.getArtifacts();
      let artifactFromConfigStore = this.ConfigStoreBeta.getArtifact();
      if (!this.selectedArtifact) {
        if (artifactFromConfigStore.name.length) {
          this.selectedArtifact = this.artifacts.filter(artifact => angular.equals(artifact, artifactFromConfigStore))[0];
        } else {
          this.selectedArtifact =  this.artifacts[0];
        }
        this.ConfigActionsFactoryBeta.setArtifact(this.selectedArtifact);
        console.log('selectedArticact', this.selectedArtifact);
        this.onArtifactChange();
        return;
      }
      let addPlugin = {name: 'Plugin Template', icon: 'fa-plus', nodeType: 'ADDPLUGINTEMPLATE', templateType: this.selectedArtifact.name};
      let getPluginTemplateNode = (type, getter) => {
        return [
          angular
            .extend(
              { pluginType: this.GLOBALS.pluginTypes[this.selectedArtifact.name][type] },
              addPlugin
            )
        ]
        .concat(this.LeftPanelStoreBeta[getter]());
      };
      this.pluginTypes[0].plugins = getPluginTemplateNode('source', 'getSources');
      this.pluginTypes[1].plugins = getPluginTemplateNode('transform', 'getTransforms');
      this.pluginTypes[2].plugins = getPluginTemplateNode('sink', 'getSinks');
    });
    this.$uibModal = $uibModal;
    this.PluginActionsFactoryBeta.fetchArtifacts({namespace: $stateParams.namespace});
  }

  fetchPlugins() {
    let params = {
      namespace: this.$stateParams.namespace,
      pipelineType: this.ConfigStoreBeta.getArtifact().name,
      version: this.rVersion.version,
      scope: this.$scope
    };
    this.PluginActionsFactoryBeta.fetchSources(params);
    this.PluginActionsFactoryBeta.fetchTransforms(params);
    this.PluginActionsFactoryBeta.fetchSinks(params);
    this.PluginActionsFactoryBeta.fetchTemplates(params);
  }

  onArtifactChange() {
    this.ConfigActionsFactoryBeta.setArtifact(this.selectedArtifact);
    this.fetchPlugins();
  }

  openFileBrowser() {
    this.$timeout(function() {
      document.getElementById('pipeline-import-config-link').click();
    });
  }

  importFile(files) {

    let generateLinearConnections = (config) => {
      let nodes = [config.source].concat(config.transforms || []).concat(config.sinks);
      let connections = [];
      let i;
      for (i=0; i<nodes.length - 1 ; i++) {
        connections.push({ from: nodes[i].name, to: nodes[i+1].name });
      }
      return connections;
    };

    let isValidArtifact = (importArtifact) => {
      return this.artifacts.filter( artifact => angular.equals(artifact, importArtifact)).length;
    };

    var reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');

    reader.onload =  (evt) => {
      var data = evt.target.result;
      var jsonData;
      try {
        jsonData = JSON.parse(data);
      } catch(e) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Syntax Error. Ill-formed pipeline configuration.'
        });
        return;
      }
      let isNotValid = this.NonStorePipelineErrorFactory.validateImportJSON(jsonData);
      if (isNotValid) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: isNotValid
        });
      } else if (!isValidArtifact(jsonData.artifact)) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Temporary message indicating invalid artifact. This should be fixed.'
        });
      } else {
        if (!jsonData.config.connections) {
          jsonData.config.connections = generateLinearConnections(jsonData.config);
        }
        this.$state.go('hydrator-beta.create', { data: jsonData });
      }
    };
  }

  showTemplates() {
    let templateType = this.ConfigStoreBeta.getArtifact().name;
    this.$uibModal.open({
      templateUrl: '/assets/features/hydrator-beta/templates/create/popovers/pre-configured-batch-list.html',
      size: 'lg',
      backdrop: true,
      keyboard: true,
      controller: 'PreConfiguredControllerBeta',
      controllerAs: 'PreConfiguredControllerBeta',
      windowTopClass: 'hydrator-template-modal',
      resolve: {
        rTemplateType: () => templateType
      }
    });
  }
  onLeftSidePanelItemClicked(event, node) {
    event.stopPropagation();
    if (node.nodeType === 'ADDPLUGINTEMPLATE') {
      this.createPluginTemplate(node, 'create');
    } else if(node.action === 'deleteTemplate') {
      this.deletePluginTemplate(node.contentData);
    } else if(node.action === 'editTemplate') {
      this.createPluginTemplate(node.contentData, 'edit');
    } else {
      this.addPluginToCanvas(event, node);
    }
  }

  deletePluginTemplate(node) {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydrator-beta/templates/create/popovers/plugin-delete-confirmation.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'plugin-template-delete-confirm-modal',
        controller: ['$scope', 'mySettings', '$stateParams', 'myAlertOnValium', 'PluginActionsFactoryBeta', function($scope, mySettings, $stateParams, myAlertOnValium, PluginActionsFactoryBeta) {
          $scope.templateName = node.pluginTemplate;
          $scope.ok = () => {
            mySettings.get('pluginTemplates', true)
              .then( (res) => {
                delete res[$stateParams.namespace][node.templateType][node.pluginType][node.pluginTemplate];
                return mySettings.set('pluginTemplates', res);
              })
              .then( () => {
                myAlertOnValium.show({
                  type: 'success',
                  content: 'Successfully deleted template ' + node.pluginTemplate
                });
                PluginActionsFactoryBeta.fetchTemplates({namespace: $stateParams.namespace});
              });
            $scope.$close();
          };
        }]
      });
  }

  createPluginTemplate(node, mode) {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydrator-beta/templates/create/popovers/plugin-templates.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'plugin-templates-modal',
        controller: ['$scope', 'PluginTemplateStoreBeta', 'PluginTemplateActionBeta', 'PluginActionsFactoryBeta', ($scope, PluginTemplateStoreBeta, PluginTemplateActionBeta, PluginActionsFactoryBeta) => {
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
              PluginActionsFactoryBeta.fetchTemplates({namespace: this.$stateParams.namespace});
              $scope.$close();
            }
          });
        }],
      })
      .rendered
      .then(() => {
        this.PluginTemplateActionBeta.init({
          templateType: node.templateType,
          pluginType: node.pluginType,
          mode: mode === 'edit'? 'edit': 'create',
          templateName: node.pluginTemplate,
          pluginName: node.pluginName
        });
      });
  }
  addPluginToCanvas(event, node) {
    var item = this.LeftPanelStoreBeta.getSpecificPluginVersion(node);
    this.NodesActionsFactoryBeta.resetSelectedNode();
    this.LeftPanelStoreBeta.updatePluginDefaultVersion(item);

    let name = item.name || item.pluginTemplate;

    let filteredNodes = this.ConfigStoreBeta
                    .getNodes()
                    .filter( node => (node.plugin.label ? node.plugin.label.includes(name) : false) );
    let config;
    if (item.pluginTemplate) {
      config = {
        plugin: {
          label: (filteredNodes.length > 0 ? item.pluginTemplate + (filteredNodes.length+1): item.pluginTemplate),
          name: item.pluginName,
          artifact: item.artifact,
          properties: item.properties,
        },
        icon: this.MyDAGFactoryBeta.getIcon(item.pluginName),
        type: item.pluginType,
        outputSchema: item.outputSchema,
        inputSchema: item.inputSchema,
        pluginTemplate: item.pluginTemplate,
        lock: item.lock
      };
    } else {
      config = {
        plugin: {
          label: (filteredNodes.length > 0 ? item.name + (filteredNodes.length+1): item.name),
          artifact: item.artifact,
          name: item.name,
          properties: {}
        },
        icon: item.icon,
        description: item.description,
        type: item.type,
        warning: true
      };
    }
    this.NodesActionsFactoryBeta.addNode(config);
  }
}

LeftPanelControllerBeta.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'LeftPanelStoreBeta', 'LeftPanelActionsFactoryBeta', 'PluginActionsFactoryBeta', 'ConfigStoreBeta', 'ConfigActionsFactoryBeta', 'MyDAGFactoryBeta', 'NodesActionsFactoryBeta', 'NonStorePipelineErrorFactory', 'HydratorServiceBeta', '$rootScope', '$uibModal', '$timeout', 'myAlertOnValium', '$state', 'PluginTemplateActionBeta', 'mySettings'];
angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('LeftPanelControllerBeta', LeftPanelControllerBeta);
