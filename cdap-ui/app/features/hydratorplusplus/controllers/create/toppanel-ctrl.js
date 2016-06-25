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
  constructor($stateParams, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, $uibModal, HydratorPlusPlusConsoleActions, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusPreviewStore, HydratorPlusPlusPreviewActions, $scope, $interval, myPipelineApi, $state, MyCDAPDataSource, GLOBALS, myAlertOnValium) {
    'ngInject';

    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusConsoleActions = HydratorPlusPlusConsoleActions;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.parsedDescription = this.HydratorPlusPlusConfigStore.getDescription();
    this.previewStore = HydratorPlusPlusPreviewStore;
    this.previewActions = HydratorPlusPlusPreviewActions;
    this.$interval = $interval;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.$scope = $scope;
    this.dataSrc = new MyCDAPDataSource($scope);
    this.GLOBALS = GLOBALS;
    this.myAlertOnValium = myAlertOnValium;

    this.canvasOperations = [
      {
        name: 'Preview',
        icon: 'fa fa-eye',
        fn: this.togglePreviewMode.bind(this)
      },
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

    this.previewMode = false;
    this.previewStartTime = null;
    this.displayDuration = {
      minutes: '00',
      seconds: '00'
    };
    this.previewTimerInterval = null;

    var sub = this.previewStore.subscribe(() => {
      let state = this.previewStore.getState().preview;
      this.previewMode = state.isPreviewModeEnabled;
    });

    this.$scope.$on('$destroy', () => {
      sub();
      this.$interval.cancel(this.previewTimerInterval);
    });
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

  startTimer() {
    this.previewTimerInterval = this.$interval(() => {
      let duration = (new Date() - this.previewStartTime) / 1000;
      duration = duration >= 0 ? duration : 0;

      let minutes = Math.floor(duration / 60);
      let seconds = Math.floor(duration % 60);
      seconds = seconds < 10 ? '0' + seconds : seconds;
      minutes = minutes < 10 ? '0' + minutes : minutes;

      this.displayDuration = {
        minutes: minutes,
        seconds: seconds
      };
    }, 500);
  }
  stopTimer() {
    this.$interval.cancel(this.previewTimerInterval);
  }

  runPreview() {
    let startTime = new Date();
    this.previewStartTime = startTime;
    this.startTimer();
    this.previewStore.dispatch(
      this.previewActions.setPreviewStartTime(startTime)
    );

    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };


    // GENERATING PREVIEW CONFIG
    // This might/should be extracted out to a factory

    let pipelineConfig = this.HydratorPlusPlusConfigStore.getConfigForExport();
    /**
     *  This is a cheat way for generating preview for the entire pipeline
     **/

    let previewConfig = {
      startStages: [],
      endStages: [],
      // useSinks: [], // we are not using sinks for now
      numOfRecords: 25
    };

    // Get start stages and end stages
    // Current implementation:
    //    - start stages mean sources
    //    - end stages mean sinks
    angular.forEach(pipelineConfig.config.stages, (node) => {
      if (this.GLOBALS.pluginConvert[node.plugin.type] === 'source') {
        previewConfig.startStages.push(node.name);
      } else if (this.GLOBALS.pluginConvert[node.plugin.type] === 'sink') {
        previewConfig.endStages.push(node.name);
      }
    });
    pipelineConfig.config.preview = previewConfig;

    if (previewConfig.startStages.length === 0 || previewConfig.endStages.length === 0) {
      this.HydratorPlusPlusConsoleActions.addMessage({
        type: 'danger',
        content: 'Please add a source and sink to the pipeline'
      });
      return;
    }

    this.myPipelineApi.runPreview(params, pipelineConfig).$promise
      .then((res) => {
        this.previewStore.dispatch(
          this.previewActions.setPreviewId(res.preview)
        );

        this.startPollPreviewStatus(res.preview);
      }, (err) => {
        this.stopTimer();
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  startPollPreviewStatus(previewId) {
    this.dataSrc.poll({
      _cdapNsPath: '/previews/' + previewId + '/status',
      interval: 5000
    }, (res) => {
      if (res.status !== 'RUNNING') {
        this.stopTimer();
        this.dataSrc.stopPoll(res.__pollId__);

        if (res.status === 'COMPLETED') {
          this.myAlertOnValium.show({
            type: 'success',
            content: 'Pipeline preview is finished.'
          });
        } else {
          this.myAlertOnValium.show({
            type: 'danger',
            content: 'Pipeline preview failed with status: ' + res.status
          });
        }
      }
    });
  }

  togglePreviewMode() {
    this.previewStore.dispatch(
      this.previewActions.togglePreviewMode(!this.previewMode)
    );
  }
  onExport() {
    this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    let config = angular.copy(this.HydratorPlusPlusConfigStore.getDisplayConfig());
    if (!config) {
      return;
    }
    this.$uibModal.open({
      templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/viewconfig.html',
      size: 'lg',
      keyboard: true,
      windowTopClass: 'hydrator-modal',
      controller: ['$scope', 'config', '$timeout', 'exportConfig', function($scope, config, $timeout, exportConfig) {
        var exportTimeout = null;

        $scope.config = JSON.stringify(config);
        $scope.export = function () {
          var blob = new Blob([JSON.stringify(exportConfig, null, 4)], { type: 'application/json'});
          $scope.url = URL.createObjectURL(blob);
          $scope.exportFileName = (exportConfig.name? exportConfig.name: 'noname') + '-' + exportConfig.artifact.name;
          $scope.$on('$destroy', function () {
            URL.revokeObjectURL($scope.url);
          });

          $timeout.cancel(exportTimeout);
          exportTimeout = $timeout(function() {
            document.getElementById('pipeline-export-config-link').click();
          });
        };

        $scope.$on('$destroy', () => {
          $timeout.cancel(exportTimeout);
        });
      }],
      resolve: {
        config: () => config,
        exportConfig: () => this.HydratorPlusPlusConfigStore.getConfigForExport()
      }
    });
  }
  onSaveDraft() {
    this.HydratorPlusPlusConfigActions.saveAsDraft();
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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusTopPanelCtrl', HydratorPlusPlusTopPanelCtrl);
