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

class HydratorPlusPlusLeftPanelCtrl {
  constructor($scope, $stateParams, rVersion, GLOBALS, HydratorPlusPlusLeftPanelStore, HydratorPlusPlusPluginActions, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, DAGPlusPlusFactory, DAGPlusPlusNodesActionsFactory, NonStorePipelineErrorFactory, $uibModal, myAlertOnValium, $state, $q, rArtifacts, $timeout, PluginTemplatesDirActions) {
    this.$state = $state;
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.HydratorPlusPlusLeftPanelStore = HydratorPlusPlusLeftPanelStore;
    this.HydratorPlusPlusPluginActions = HydratorPlusPlusPluginActions;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.PluginTemplatesDirActions = PluginTemplatesDirActions;
    this.rVersion = rVersion;
    this.$timeout = $timeout;
    this.myAlertOnValium = myAlertOnValium;
    this.$q = $q;

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

    this.artifacts = rArtifacts;
    let configStoreArtifact = this.HydratorPlusPlusConfigStore.getArtifact();
    this.selectedArtifact = rArtifacts.filter( ar => ar.name === configStoreArtifact.name)[0];
    console.log('setting: ', this.selectedArtifact);
    this.fetchPlugins();
    this.HydratorPlusPlusLeftPanelStore.registerOnChangeListener(() => {
      let addPlugin = {
        name: 'Plugin Template',
        icon: 'fa-plus',
        nodeClass: 'add-plugin-template',
        nodeType: 'ADDPLUGINTEMPLATE',
        templateType: this.selectedArtifact.name
      };
      let getPluginTemplateNode = (type, getter) => {
        return [
          angular
            .extend(
              { pluginType: this.GLOBALS.pluginTypes[this.selectedArtifact.name][type] },
              addPlugin
            )
        ]
        .concat(this.HydratorPlusPlusLeftPanelStore[getter]());
      };
      this.pluginTypes[0].plugins = getPluginTemplateNode('source', 'getSources');
      this.pluginTypes[1].plugins = getPluginTemplateNode('transform', 'getTransforms');
      this.pluginTypes[2].plugins = getPluginTemplateNode('sink', 'getSinks');
    });
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusPluginActions.fetchArtifacts({namespace: $stateParams.namespace});
  }

  fetchPlugins() {
    this.artifactToRevert = this.selectedArtifact;
    let params = {
      namespace: this.$stateParams.namespace,
      pipelineType: this.selectedArtifact.name,
      version: this.rVersion.version,
      scope: this.$scope
    };
    this.HydratorPlusPlusPluginActions.fetchSources(params);
    this.HydratorPlusPlusPluginActions.fetchTransforms(params);
    this.HydratorPlusPlusPluginActions.fetchSinks(params);
    this.HydratorPlusPlusPluginActions.fetchTemplates(params);
  }

  onArtifactChange() {
    console.log('On Artifact baslasduas');
    this._checkAndShowConfirmationModalOnDirtyState()
      .then(saveState => {
        if (saveState) {
          console.log('setting1: ', this.selectedArtifact);
          this.selectedArtifact = this.artifactToRevert;
        } else {
          this.$state.go('hydratorplusplus.create', {
            artifactType: this.selectedArtifact.name,
            data: null
          });
        }
      });
  }

  openFileBrowser() {
    let fileBrowserClickCB = () => {
      document.getElementById('pipeline-import-config-link').click();
    };
    this._checkAndShowConfirmationModalOnDirtyState(() => {}, () => this.$timeout(fileBrowserClickCB));
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
        this.$state.go('hydratorplusplus.create', { data: jsonData });
      }
    };
  }

  showTemplates() {
    let templateType = this.HydratorPlusPlusConfigStore.getArtifact().name;
    let openTemplatesPopup = () => {
      this.$uibModal.open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/pre-configured-batch-list.html',
        size: 'lg',
        backdrop: true,
        keyboard: true,
        controller: 'HydratorPlusPlusPreConfiguredCtrl',
        controllerAs: 'HydratorPlusPlusPreConfiguredCtrl',
        windowTopClass: 'hydrator-template-modal',
        resolve: {
          rTemplateType: () => templateType
        }
      });
    };
    this._checkAndShowConfirmationModalOnDirtyState()
      .then(saveState =>{
        if (!saveState) {
          openTemplatesPopup();
        }
      });
  }

  _checkAndShowConfirmationModalOnDirtyState(yesCb, noCb) {
    let isSavePipeline = true;
    let isStoreDirty = this.HydratorPlusPlusConfigStore.getIsStateDirty();
    if (isStoreDirty) {
      return this.$uibModal.open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/canvas-overwrite-confirmation.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        controller: ['$scope', function($scope) {
          $scope.yes = () => {
            if (yesCb) {
              yesCb();
            } else {
              isSavePipeline = true;
            }
            $scope.$close();
          };
          $scope.no = () => {
            if (noCb) {
              noCb();
            } else {
              isSavePipeline = false;
            }
            $scope.$close();
          };
        }]
      })
      .closed
      .then(() => {
        return isSavePipeline;
      });
    } else {
      if (noCb) {
        noCb();
      } else {
        return this.$q.when(false);
      }
    }
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
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/plugin-delete-confirmation.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'plugin-template-delete-confirm-modal',
        controller: 'PluginTemplatesDeleteCtrl',
        resolve: {
          rNode: () => node
        }
      });
  }

  createPluginTemplate(node, mode) {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/plugin-templates.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'plugin-templates-modal',
        controller: 'PluginTemplatesCreateEditCtrl'
      })
      .rendered
      .then(() => {
        this.PluginTemplatesDirActions.init({
          templateType: node.templateType,
          pluginType: node.pluginType,
          mode: mode === 'edit'? 'edit': 'create',
          templateName: node.pluginTemplate,
          pluginName: node.pluginName
        });
      });
  }
  addPluginToCanvas(event, node) {
    var item = this.HydratorPlusPlusLeftPanelStore.getSpecificPluginVersion(node);
    this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    this.HydratorPlusPlusLeftPanelStore.updatePluginDefaultVersion(item);

    let name = item.name || item.pluginTemplate;

    let filteredNodes = this.HydratorPlusPlusConfigStore
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
        icon: this.DAGPlusPlusFactory.getIcon(item.pluginName),
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
    this.DAGPlusPlusNodesActionsFactory.addNode(config);
  }
}

HydratorPlusPlusLeftPanelCtrl.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'HydratorPlusPlusLeftPanelStore', 'HydratorPlusPlusPluginActions', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', 'DAGPlusPlusFactory', 'DAGPlusPlusNodesActionsFactory', 'NonStorePipelineErrorFactory',  '$uibModal', 'myAlertOnValium', '$state', '$q', 'rArtifacts', '$timeout', 'PluginTemplatesDirActions'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusLeftPanelCtrl', HydratorPlusPlusLeftPanelCtrl);
