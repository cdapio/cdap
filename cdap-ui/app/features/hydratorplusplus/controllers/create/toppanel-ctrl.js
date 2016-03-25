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

class HydratorPlusPlusTopPanelCtrl{
  constructor(GLOBALS, $stateParams, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, $uibModal, HydratorPlusPlusConsoleActions, DAGPlusPlusNodesActionsFactory) {

    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusConsoleActions = HydratorPlusPlusConsoleActions;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.parsedDescription = this.HydratorPlusPlusConfigStore.getDescription();

    this.canvasOperations = [
      {
        name: 'Export',
        icon: 'icon-export',
        fn: this.onExport.bind(this)
      },
      {
        name: 'Save',
        icon: 'icon-savedraft',
        fn: this.onSaveDraft.bind(this)
      },
      {
        name: 'Validate',
        icon: 'icon-validate',
        fn: this.onValidate.bind(this)
      },
      {
        name: 'Publish',
        icon: 'icon-publish',
        fn: this.onPublish.bind(this)
      }
    ];
    this.$stateParams = $stateParams;
    this.setState();
    this.HydratorPlusPlusConfigStore.registerOnChangeListener(this.setState.bind(this));
  }
  setMetadata(metadata) {
    this.state.metadata = metadata;
  }
  setState() {
    this.state = {
      metadata: {
        name: this.HydratorPlusPlusConfigStore.getName(),
        description: this.HydratorPlusPlusConfigStore.getDescription()
      },
      artifact: this.HydratorPlusPlusConfigStore.getArtifact()
    };
  }

  openMetadata() {
    this.metadataExpanded = true;
  }
  resetMetadata() {
    this.setState();
    this.metadataExpanded = false;
  }
  saveMetadata() {
    this.HydratorPlusPlusConfigActions.setMetadataInfo(this.state.metadata.name, this.state.metadata.description);
    if (this.state.metadata.description) {
      this.parsedDescription = this.state.metadata.description.replace(/\n/g, ' ');
      this.tooltipDescription = this.state.metadata.description.replace(/\n/g, '<br />');
    } else {
      this.parsedDescription = '';
      this.tooltipDescription = '';
    }
    this.metadataExpanded = false;
  }
  onEnterOnMetadata(event) {
    // Save when user hits ENTER key.
    if (event.keyCode === 13) {
      this.saveMetadata();
      this.metadataExpanded = false;
    } else if (event.keyCode === 27) {
      // Reset if the user hits ESC key.
      this.resetMetadata();
    }
  }

  onExport() {
    this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    let config = angular.copy(this.HydratorPlusPlusConfigStore.getDisplayConfig());
    if (!config) {
      return;
    }
    this.$uibModal.open({
      templateUrl: '/assets/features/hydrator/templates/create/popovers/viewconfig.html',
      size: 'lg',
      keyboard: true,
      controller: ['$scope', 'config', '$timeout', 'exportConfig', function($scope, config, $timeout, exportConfig) {
        $scope.config = JSON.stringify(config);
        $scope.export = function () {
          var blob = new Blob([JSON.stringify(exportConfig, null, 4)], { type: 'application/json'});
          $scope.url = URL.createObjectURL(blob);
          $scope.exportFileName = (exportConfig.name? exportConfig.name: 'noname') + '-' + exportConfig.artifact.name;
          $scope.$on('$destroy', function () {
            URL.revokeObjectURL($scope.url);
          });
          $timeout(function() {
            document.getElementById('pipeline-export-config-link').click();
          });
        };
      }],
      resolve: {
        config: () => config,
        exportConfig: () => this.HydratorPlusPlusConfigStore.getConfigForExport()
      }
    });
  }
  onSaveDraft() {
    var config = this.HydratorPlusPlusConfigStore.getState();
    if (!config.name) {
      this.HydratorPlusPlusConsoleActions.addMessage({
        type: 'error',
        content: this.GLOBALS.en.hydrator.studio.error['MISSING-NAME']
      });
      return;
    }
    this.HydratorPlusPlusConfigActions.saveAsDraft(config);
  }
  onValidate() {
    this.HydratorPlusPlusConsoleActions.resetMessages();
    let isStateValid = this.HydratorPlusPlusConfigStore.validateState(true);
    if (isStateValid) {
      this.HydratorPlusPlusConsoleActions.addMessage({
        type: 'success',
        content: 'Validation success! Pipeline ' + this.HydratorPlusPlusConfigStore.getName() + ' is valid.'
      });
    }
  }
  onPublish() {
    this.HydratorPlusPlusConfigActions.publishPipeline();
  }
}

HydratorPlusPlusTopPanelCtrl.$inject = ['GLOBALS', '$stateParams', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', '$uibModal', 'HydratorPlusPlusConsoleActions', 'DAGPlusPlusNodesActionsFactory'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusTopPanelCtrl', HydratorPlusPlusTopPanelCtrl);
