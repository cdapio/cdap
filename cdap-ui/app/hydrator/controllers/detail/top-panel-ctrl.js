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

class HydratorDetailTopPanelController {
  constructor(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium, myPipelineExportModalService, myPipelineApi, myHelpers, myPreferenceApi, $q) {
    this.GLOBALS = GLOBALS;
    this.myPipelineExportModalService = myPipelineExportModalService;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.moment = moment;
    this.$scope = $scope;
    this.$timeout = $timeout;
    this.scheduleTimeout = null;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusDetailRunsStore = HydratorPlusPlusDetailRunsStore;
    this.HydratorPlusPlusDetailActions = HydratorPlusPlusDetailActions;
    this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.myPipelineApi = myPipelineApi;
    this.myHelpers = myHelpers;
    this.myLoadingService = myLoadingService;
    this.myPreferenceApi = myPreferenceApi;
    this.$q = $q;
    this.app = {
      name: HydratorPlusPlusDetailNonRunsStore.getPipelineName(),
      description: this.config.description,
      type: this.config.artifact.name
    };
    this.runPlayer = {
      view: false,
      action: null
    };
    this.viewSettings = false;
    this.viewLogs = false;
    this.pipelineType = HydratorPlusPlusDetailNonRunsStore.getPipelineType();
    this.tooltipDescription = (this.app.description && this.app.description.replace(/\n/g, '<br />')) || '' ;
    this.setState();
    this.setAppStatus();
    var appType = HydratorPlusPlusDetailNonRunsStore.getAppType();
    if (GLOBALS.etlBatchPipelines.indexOf(appType) !== -1) {
      HydratorPlusPlusDetailActions.fetchScheduleStatus(
        HydratorPlusPlusDetailRunsStore.getApi(),
        HydratorPlusPlusDetailRunsStore.getScheduleParams()
      );
    }
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(() => {
      this.setAppStatus();
      this.setState();
    });
    HydratorPlusPlusDetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));

    this.macrosMap = {};
    this.runTimeWidgetConfig = {
      'widget-type': 'map',
      'widget-attributes': {
        'key-placeholder': 'Argument name',
        'value-placeholder': 'Argument value'
      }
    };

    this.fetchMacros();
    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.scheduleTimeout);
    });
  }

  fetchMacros() {

    const parseMacros = macrosSpec => {
      let macrosObj = {};
      for(let i = 0; i < macrosSpec.length; i++){
        if(this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties') &&
          this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties').length > 0){
            macrosObj[this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties')] = '';
        }
      }
      return macrosObj;
    };

    let {namespace, app} = this.HydratorPlusPlusDetailRunsStore.getParams();
    let preferenceParams = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.pipelineId,
      scope: this.$scope
    };
    const errorHandler = err => console.log('ERROR', err);

    return this.myPipelineApi
      .fetchMacros({
        namespace,
        pipeline: app
      })
      .$promise
      .then(
        (res) => {
          this.macrosMap = parseMacros(res);
          return this.macrosMap;
        },
        err => this.macroError = err
      )
      .then(
        () => this.myPreferenceApi.getAppPreference(preferenceParams).$promise,
        errorHandler
      )
      .then(
        res => this.syncPreferencesStoreWithMacros(res),
        errorHandler
      );
  }

  syncPreferencesStoreWithMacros(appPreferences = {}) {
    try {
      appPreferences = JSON.parse(angular.toJson(appPreferences));
    } catch(e) {
      console.log('ERROR: ', e);
      appPreferences = {};
    }
    this.macrosMap = Object.assign({}, this.macrosMap, appPreferences);
  }

  isValidToStartOrSchedule() {
    if (Object.keys(this.macrosMap).length === 0) {
      return true;
    }

    let res = true;

    for(let key in this.macrosMap){
      if(!this.macrosMap[key]){
        res = false;
      }
    }
    return res;
  }

  setState() {
    var runs = this.HydratorPlusPlusDetailRunsStore.getRuns();
    var status, i;
    var lastRunDuration;
    for (i=0 ; i<runs.length; i++) {
      status = runs[i].status;
      if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
        this.lastFinished = runs[i];
        break;
      }
    }
    if (this.lastFinished) {
      lastRunDuration = this.lastFinished.end - this.lastFinished.start;
      this.lastRunTime = this.moment.utc(lastRunDuration * 1000).format('HH:mm:ss');
    } else {
      this.lastRunTime = 'N/A';
    }
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setAppStatus() {
    this.appStatus = this.HydratorPlusPlusDetailRunsStore.getStatus();
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setScheduleStatus() {
    this.scheduleStatus = this.HydratorPlusPlusDetailNonRunsStore.getScheduleStatus();
  }
  isGreenStatus() {
    var greenStatus = ['COMPLETED', 'RUNNING', 'SCHEDULED', 'STARTING'];
    if (greenStatus.indexOf(this.appStatus) > -1) {
      return true;
    } else {
      return false;
    }
  }
  exportConfig() {
    let config = angular.copy(this.HydratorPlusPlusDetailNonRunsStore.getConfigJson());
    config.stages = config.stages.map( stage => ({
      name: stage.name,
      plugin: stage.plugin
    }));
    let exportConfig = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.myPipelineExportModalService.show(config, exportConfig);
  }
  do(action) {
    switch(action) {
      case 'Start':
        this.fetchMacros()
          .then(() => {
            this.runPlayer.view = true;
            this.runPlayer.action = 'STARTING';
          }, (err) => {console.log('Error: ', err); });
        break;
      case 'Schedule':
        this.fetchMacros()
          .then(() => {
            this.runPlayer.view = true;
            this.runPlayer.action = 'SCHEDULING';
          });
        break;
      case 'Suspend':
        this.suspendPipeline();
        break;
      case 'Stop':
        this.appStatus = 'STOPPING';
        this.stopPipeline();
        break;
      case 'Delete':
        this.myLoadingService.showLoadingIcon();
        var params = this.HydratorPlusPlusDetailRunsStore.getParams();
        params = {
          namespace: params.namespace,
          pipeline: params.app
        };
        this.HydratorPlusPlusDetailActions
          .deletePipeline(params)
          .then(
            () => {
              this.$state.go('hydrator.list');
              this.myLoadingService.hideLoadingIcon();
            },
            (err) => {
              this.myLoadingService.hideLoadingIcon();
              this.$timeout(() => {
                this.myAlertOnValium.show({
                  type: 'danger',
                  title: 'Unable to delete Pipeline',
                  content: err.data
                });
              });
            }
          );
    }
  }
  startPipeline() {
    this.appStatus = 'STARTING';
    this.runPlayer.view = false;
    this.runPlayer.action = null;
    this.HydratorPlusPlusDetailActions.startPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams(),
      this.macrosMap
    )
    .then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to start a new run',
          content: angular.isObject(err) ? err.data: err
        });
      }
    )
    .then(
      () => this.fetchMacros()
    );
  }
  stopPipeline() {
    this.HydratorPlusPlusDetailActions.stopPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams()
    ).then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to stop the current run',
          content: angular.isObject(err) ? err.data: err
        });
      }
    );
  }

  schedulePipeline() {
    this.scheduleStatus = 'SCHEDULING';
    this.scheduleLoading = true;
    let preferenceParams = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.pipelineId,
      scope: this.$scope
    };

    this.myPreferenceApi
      .setAppPreference(preferenceParams, this.macrosMap)
      .$promise
      .then(
        () => {
          return this.HydratorPlusPlusDetailActions.schedulePipeline(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          )
            .then(() => {
              if (this.scheduleTimeout) {
                this.$timeout.cancel(this.scheduleTimeout);
              }
              this.scheduleTimeout = this.$timeout(() => {
                this.scheduleLoading = false;
              }, 1000);
            });
        },
        (err) => {
          this.scheduleLoading = false;
          this.macroError = angular.isObject(err) ? (err.data || 'Error saving runtime arguments in preferences store') : err;
          this.setScheduleStatus();
          return this.$q.reject();
        }
      )
      .then(
        () => {
          this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          );
        },
        (err) => {
          if (!this.macroError) {
            this.myAlertOnValium.show({
              type: 'danger',
              title: 'Unable to schedule the pipeline',
              content: angular.isObject(err) ? err.data : err
            });
          }
          this.setScheduleStatus();
          return this.$q.reject();
        }
      )
      .then(
        () => {
          this.runPlayer.view = false;
          this.runPlayer.action = null;
          this.fetchMacros();
        }
      );
  }
  suspendPipeline() {
    this.scheduleLoading = true;
    this.HydratorPlusPlusDetailActions.suspendSchedule(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
    )
      .then(
        () => {
          this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          );

          if (this.scheduleTimeout) {
            this.$timeout.cancel(this.scheduleTimeout);
          }
          this.scheduleTimeout = this.$timeout(() => {
            this.scheduleLoading = false;
          }, 1000);
        },
        (err) => {
          this.myAlertOnValium.show({
            type: 'danger',
            title: 'Unable to suspend the pipeline',
            content: angular.isObject(err) ? err.data: err
          });
          this.scheduleLoading = false;
        }
      );
  }
}

HydratorDetailTopPanelController.$inject = ['HydratorPlusPlusDetailRunsStore', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusDetailActions', 'GLOBALS', '$state', 'myLoadingService', '$timeout', '$scope', 'moment', 'myAlertOnValium', 'myPipelineExportModalService', 'myPipelineApi', 'myHelpers', 'myPreferenceApi', '$q'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailTopPanelController', HydratorDetailTopPanelController);
