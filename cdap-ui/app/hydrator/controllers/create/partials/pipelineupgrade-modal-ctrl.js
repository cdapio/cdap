/*
 * Copyright © 2017 Cask Data, Inc.
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
  .controller('PipelineUpgradeModalController', function ($scope, rPipelineConfig, HydratorUpgradeService, $rootScope, HydratorPlusPlusConfigStore, $state, DAGPlusPlusFactory, GLOBALS, HydratorPlusPlusLeftPanelStore) {
    let eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);
    let globalEvents = window.CaskCommon.globalEvents;

    this.pipelineConfig = rPipelineConfig;
    this.cdapVersion = $rootScope.cdapVersion;

    this.pipelineArtifact = HydratorUpgradeService.checkPipelineArtifactVersion(rPipelineConfig);

    this.problematicStages = [];
    this.canUpgradeStages = [];
    let allStages = [];

    let allPostActions = [];
    this.problematicPostRunActions = [];
    this.fixAllDisabled = true;

    // missing artifacts map
    this.missingArtifactsMap = {};

    this.loading = false;

    const checkStages = () => {
      if (this.loading) { return; }
      this.loading = true;

      HydratorUpgradeService.getErrorStages(rPipelineConfig)
        .then((transformedStages) => {

          allStages = transformedStages.stages.map((stage) => {
            stage.icon = DAGPlusPlusFactory.getIcon(stage.stageInfo.plugin.name.toLowerCase());
            stage.type = GLOBALS.pluginConvert[stage.stageInfo.plugin.type];
            return stage;
          });

          allPostActions = transformedStages.postActions.map((stage) => {
            stage.icon = DAGPlusPlusFactory.getIcon(stage.stageInfo.plugin.name.toLowerCase());
            stage.type = 'postaction';
            return stage;
          });

          this.problematicStages = [];
          this.canUpgradeStages = [];
          this.problematicPostRunActions = [];
          this.missingArtifactsMap = {};

          transformedStages.stages.forEach((artifact) => {
            if (artifact.error === 'NOTFOUND') {
              let plugin = artifact.stageInfo.plugin;
              let mapKey = `${plugin.name}-${plugin.type}-${plugin.artifact.name}-${plugin.artifact.version}`;

              this.missingArtifactsMap[mapKey] = artifact;
            } else if (artifact.error === 'CAN_UPGRADE') {
              artifact.upgrade = true;
              this.canUpgradeStages.push(artifact);
            } else if (artifact.error) {
              this.problematicStages.push(artifact);
            }
          });

          transformedStages.postActions.forEach((artifact) => {
            if (artifact.error === 'NOTFOUND') {
              let plugin = artifact.stageInfo.plugin;
              let mapKey = `${plugin.name}-${plugin.type}-${plugin.artifact.name}-${plugin.artifact.version}`;

              this.missingArtifactsMap[mapKey] = artifact;
            } else if (artifact.error) {
              this.problematicPostRunActions.push(artifact);
            }
          });

          this.fixAllDisabled = Object.keys(this.missingArtifactsMap).length > 0;

          if (
            this.problematicStages.length === 0 &&
            this.pipelineArtifact &&
            this.canUpgradeStages.length === 0 &&
            this.problematicPostRunActions.length === 0 &&
            !this.fixAllDisabled
          ) {
            HydratorPlusPlusConfigStore.setState(HydratorPlusPlusConfigStore.getDefaults());
            $state.go('hydrator.create', { data: rPipelineConfig });
          } else {
            this.loading = false;
          }
        });
    };

    checkStages();

    // This store subscription can cause the fetching of the plugins list to happen twice.
    // The reason is because in LeftPanelController, we fetch the default version map
    // with a 10 seconds timeout. So if user import before 10 seconds, it will make another
    // call to fetch list of plugins
    let sub = HydratorPlusPlusLeftPanelStore.subscribe(checkStages);

    this.openMarket = () => {
      eventEmitter.emit(globalEvents.OPENMARKET);
    };

    const fix = (stagesList) => {
      return stagesList.map((stage) => {
        let updatedStageInfo = stage.stageInfo;

        if (stage.error && stage.error === 'NOTFOUND') {
          updatedStageInfo.error = true;
          updatedStageInfo.errorCount = 1;
          updatedStageInfo.errorMessage = 'Plugin cannot be found';
        } else if (stage.error) {
          if (stage.error === 'CAN_UPGRADE' && stage.upgrade || stage.error !== 'CAN_UPGRADE') {
            updatedStageInfo.plugin.artifact = stage.suggestion;
          }
        }

        return updatedStageInfo;
      });
    };

    this.fixAll = () => {
      let newConfig = HydratorUpgradeService.upgradePipelineArtifactVersion(rPipelineConfig);

      // Making a copy here so that the information in the modal does not change when
      // we modify the artifact information
      let copyAllStages = angular.copy(allStages);
      let copyPostActions = angular.copy(allPostActions);

      let stages = fix(copyAllStages);
      let postActions = fix(copyPostActions);

      newConfig.config.stages = stages;
      newConfig.config.postActions = postActions;

      if (newConfig.__ui__) {
        delete newConfig.__ui__;
      }

      HydratorPlusPlusConfigStore.setState(HydratorPlusPlusConfigStore.getDefaults());
      $state.go('hydrator.create', { data: newConfig });
    };

    $scope.$on('$destroy', () => {
      sub();
    });
  });
