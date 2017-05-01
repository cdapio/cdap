/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

/*
  A small utility to bump the version of pipeline name while cloning the pipeline.

  Usage:
  getClonePipelineName('SamplePipeline') -> SamplePipeline_v1
  getClonePipelineName('SamplePipeline_v1') -> SamplePipeline_v2
  getClonePipelineName('SamplePipeline_v100') -> SamplePipeline_v101
  getClonePipelineName('SamplePipeline_v1_v3_v33') -> SamplePipeline_v1_v3_v34
  getClonePipelineName('SamplePipeline_22_v33') -> SamplePipeline_22_v34
  getClonePipelineName(1) -> 1_v1 (apparently this is allowed for a pipeline name).
*/

const getClonePipelineName = (name) => {
  name = typeof a === 'string' ? name : name.toString();
  let version = name.match(/(_v[\d]*)$/g);
  var existingSuffix; // For cases where pipeline name is of type 'SamplePipeline_v2_v4_v333'
  if (Array.isArray(version)) {
    version = version.pop();
    existingSuffix = version;
    version = version.replace('_v', '');
    version = '_v' + ((!isNaN(parseInt(version, 10)) ? parseInt(version, 10) : 1) + 1);
  } else {
    version = '_v1';
  }
  return name.split(existingSuffix)[0] + version;
};

class HydratorDetailTopPanelController {
  constructor(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium, myPipelineExportModalService, myPipelineApi, myHelpers, myPreferenceApi, $q, $interval, MyPipelineStatusMapper, HydratorPlusPlusHydratorService) {
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
    this.cloneConfig = _.cloneDeep(HydratorPlusPlusDetailNonRunsStore.getCloneConfig());
    this.cloneConfig.name = getClonePipelineName(this.cloneConfig.name);
    this.myPipelineApi = myPipelineApi;
    this.myHelpers = myHelpers;
    this.myLoadingService = myLoadingService;
    this.myPreferenceApi = myPreferenceApi;
    this.$q = $q;
    this.$interval = $interval;
    this.MyPipelineStatusMapper = MyPipelineStatusMapper;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
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
    this.pipelineDurationTimer = null;
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

    this.viewInCdapLink = window.getOldCDAPUrl({
      stateName: 'apps.detail.overview.status',
      stateParams: {
        namespace: $state.params.namespace,
        appId: this.app.name
      }
    });

    this.fetchMacros();
    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.scheduleTimeout);
      this.$interval.cancel(this.pipelineDurationTimer);
    });
  }

  fetchMacros() {

    const parseMacros = macrosSpec => {
      let macrosObj = {};
      for(let i = 0; i < macrosSpec.length; i++){
        if(this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties') &&
          this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties').length > 0){
            let macrosKeys = this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties');

            macrosKeys.forEach((key) => {
              macrosObj[key] = '';
            });
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
        () => this.myPreferenceApi.getAppPreferenceResolved(preferenceParams).$promise,
        errorHandler
      )
      .then(
        (res) => {
          let relevantPrefs = this.HydratorPlusPlusHydratorService.getPrefsRelevantToMacros(res, this.macrosMap);
          this.macrosMap = Object.assign({}, this.macrosMap, relevantPrefs);
        },
        errorHandler
      );
  }

  isValidToStartOrSchedule() {
    if (Object.keys(this.macrosMap).length === 0) {
      return true;
    }

    let res = true;

    for(let key in this.macrosMap){
      if (this.macrosMap.hasOwnProperty(key) && !this.macrosMap[key]) {
        res = false;
      }
    }
    return res;
  }

  setState() {
    var latestRun = this.HydratorPlusPlusDetailRunsStore.getLatestRun();
    var lastRunDuration;

    if (latestRun) {
      this.lastFinished = latestRun;
    }

    if (this.lastFinished) {

      if (this.lastFinished.status === 'RUNNING') {

        if (!this.pipelineDurationTimer) {
          this.pipelineDurationTimer = this.$interval(() => {

            if (this.lastFinished.end) {
              let endDuration = this.lastFinished.end - this.lastFinished.start;
              this.lastRunTime = typeof this.lastFinished.end === 'number' ? this.moment.utc(endDuration * 1000).format('HH:mm:ss') : 'N/A';
            } else {
              let runningDuration = new Date().getTime() - (this.lastFinished.start * 1000);
              this.lastRunTime = this.moment.utc(runningDuration).format('HH:mm:ss');
            }
          }, 1000);
        }
      }

      lastRunDuration = this.lastFinished.end - this.lastFinished.start;

      let setInitialTimer = new Date().getTime() - (this.lastFinished.start * 1000);
      this.lastRunTime = typeof this.lastFinished.end === 'number' ?
                          this.moment.utc(lastRunDuration * 1000).format('HH:mm:ss') :
                          this.moment.utc(setInitialTimer).format('HH:mm:ss');
    } else {
      this.lastRunTime = 'N/A';
    }
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }

  setAppStatus() {
    this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus(
      this.HydratorPlusPlusDetailRunsStore.getStatus());

    if (this.appStatus === 'Succeeded' || this.appStatus === 'Stopped') {
      this.$interval.cancel(this.pipelineDurationTimer);
      this.pipelineDurationTimer = null;
    }

    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setScheduleStatus() {
    this.scheduleStatus = this.MyPipelineStatusMapper.lookupDisplayStatus(this.HydratorPlusPlusDetailNonRunsStore.getScheduleStatus());
  }
  isGreenStatus() {
    var greenStatus = ['Succeeded', 'Starting', 'Scheduling', 'Stopping'];
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
    delete exportConfig.__ui__;
    this.myPipelineExportModalService.show(config, exportConfig);
  }
  do(action) {
    switch(action) {
      case 'Start':
        this.fetchMacros()
          .then(() => {
            this.runPlayer.view = true;
            this.runPlayer.action = this.MyPipelineStatusMapper.lookupDisplayStatus('STARTING');
          }, (err) => {console.log('Error: ', err); });
        break;
      case 'Schedule':
        this.fetchMacros()
          .then(() => {
            this.runPlayer.view = true;
            this.runPlayer.action = this.MyPipelineStatusMapper.lookupDisplayStatus('SCHEDULING');
          });
        break;
      case 'Suspend':
        this.suspendPipeline();
        break;
      case 'Stop':
        this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('STOPPING');
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
    this.lastRunTime = 'N/A';
    this.lastFinished = null;
    this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('STARTING');
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
    this.scheduleStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('SCHEDULING');
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

HydratorDetailTopPanelController.$inject = ['HydratorPlusPlusDetailRunsStore', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusDetailActions', 'GLOBALS', '$state', 'myLoadingService', '$timeout', '$scope', 'moment', 'myAlertOnValium', 'myPipelineExportModalService', 'myPipelineApi', 'myHelpers', 'myPreferenceApi', '$q', '$interval', 'MyPipelineStatusMapper', 'HydratorPlusPlusHydratorService'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailTopPanelController', HydratorDetailTopPanelController);
