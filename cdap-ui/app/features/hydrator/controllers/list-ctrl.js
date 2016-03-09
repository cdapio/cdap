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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorListController', function($scope, myPipelineApi, $stateParams, GLOBALS, mySettings, $state, $timeout, myHelpers, myWorkFlowApi, myWorkersApi, MyCDAPDataSource, myAppsApi, myAlertOnValium) {
    var dataSrc = new MyCDAPDataSource($scope);

    var vm = this;

    vm.pipelineList = [];
    vm.currentPage = 1;
    vm.statusCount = {
      running: 0,
      scheduled: 0,
      suspended: 0,
      draft: 0
    };

    vm.GLOBALS = GLOBALS;

    var realtime = [],
        batch = [];

    var statusMap = {};


    myPipelineApi.list({
      namespace: $stateParams.namespace
    })
      .$promise
      .then(function success(res) {
        vm.pipelineList = res;

        fetchDrafts();

        angular.forEach(vm.pipelineList, function (app) {
          app._stats = {};

          fetchRunsInfo(app);
        });

        fetchStatus();

      });


    function fetchRunsInfo(app) {
      var params = {
        namespace: $state.params.namespace,
        appId: app.id,
        scope: $scope
      };

      var api;

      if (app.artifact.name === GLOBALS.etlBatch) {
        api = myWorkFlowApi;
        params.workflowId = 'ETLWorkflow';

        batch.push({
          appId: app.id,
          programType: 'Workflow',
          programId: 'ETLWorkflow'
        });
      } else {
        api = myWorkersApi;
        params.workerId = 'ETLWorker';

        realtime.push({
          appId: app.id,
          programType: 'Worker',
          programId: 'ETLWorker'
        });
      }

      api.runs(params)
        .$promise
        .then(function (runs) {
          app._stats.numRuns = runs.length;
          app._stats.lastStartTime = runs.length > 0 ? runs[0].start : 'N/A';

          for (var i = 0; i < runs.length; i++) {
            var status = runs[i].status;

            if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
              app._latest = runs[i];
              break;
            }
          }

        });
    }

    function fetchStatus() {
      // fetching ETL Batch statuses
      dataSrc.request({
        _cdapNsPath: '/status',
        method: 'POST',
        body: batch
      })
      .then(function (res) {
        angular.forEach(res, function (app) {
          if (app.status === 'RUNNING') {
            statusMap[app.appId] = 'Running';
            vm.statusCount.running++;
          } else {

            myWorkFlowApi.getScheduleStatus({
              namespace: $state.params.namespace,
              appId: app.appId,
              scheduleId: 'etlWorkflow',
              scope: $scope
            })
              .$promise
              .then(function (schedule) {
                if (schedule.status === 'SCHEDULED') {
                  statusMap[app.appId] = 'Scheduled';
                  vm.statusCount.scheduled++;
                } else {
                  statusMap[app.appId] = 'Suspended';
                  vm.statusCount.suspended++;
                }
                updateStatusAppObject();
              });
          }
        });

        updateStatusAppObject();
      }); // end of ETL Batch


      //fetching ETL Realtime statuses
      dataSrc.request({
        _cdapNsPath: '/status',
        method: 'POST',
        body: realtime
      })
      .then(function (res) {
        angular.forEach(res, function (app) {
          if (app.status === 'RUNNING') {
            statusMap[app.appId] = 'Running';
            vm.statusCount.running++;
          } else {
            statusMap[app.appId] = 'Suspended';
            vm.statusCount.suspended++;
          }
        });

        updateStatusAppObject();
      });

    }

    function updateStatusAppObject() {
      angular.forEach(vm.pipelineList, function (app) {
        app._status = app._status || statusMap[app.id];
      });
    }

    function fetchDrafts() {
      mySettings.get('hydratorDrafts')
        .then(function(res) {
          let draftsList = myHelpers.objectQuery(res, $stateParams.namespace);
          if (!angular.isObject(draftsList)) {
            return;
          }
          if (Object.keys(draftsList).length) {
            angular.forEach(res[$stateParams.namespace], function(value, key) {
              vm.statusCount.draft++;
              vm.pipelineList.push({
                isDraft: true,
                name: value.name,
                id: (value.__ui__  && value.__ui__.draftId) ? value.__ui__.draftId : key,
                artifact: value.artifact,
                description: myHelpers.objectQuery(value, 'description'),
                _status: 'Draft',
                _stats: {
                  numRuns: 'N/A',
                  lastStartTime: 'N/A'
                }
              });
            });
          }
        });
    }

    vm.deleteDraft = function(draftId) {
      let draftName;
      mySettings.get('hydratorDrafts')
        .then(function(res) {
          let draft = myHelpers.objectQuery(res, $stateParams.namespace, draftId);
          if (draft) {
            draftName = draft.name;
            delete res[$stateParams.namespace][draftId];
          }
          return mySettings.set('hydratorDrafts', res);
        })
        .then(
          function success() {
            $state.reload()
              .then(function() {
                myAlertOnValium.show({
                  type: 'success',
                  content: 'Pipeline draft ' + draftName + ' deleted successfully'
                });
              });
          },
          function error() {
            $state.reload()
              .then(function() {
                myAlertOnValium.show({
                  type: 'danger',
                  content: 'Pipeline draft ' + draftName + ' delete failed'
                });
              });
          });
    };


    vm.deleteApp = function (appId) {
      var deleteParams = {
        namespace: $state.params.namespace,
        appId: appId,
        scope: $scope
      };
      myAppsApi.delete(deleteParams)
        .$promise
        .then(function success () {
          $state.reload()
            .then(function() {
              myAlertOnValium.show({
                type: 'success',
                content: 'Pipeline ' + appId + ' deleted successfully'
              });
            });
        }, function error () {
          $state.reload()
            .then(function() {
              myAlertOnValium.show({
                type: 'danger',
                content:  'Pipeline ' + appId + ' delete failed'
              });
            });
        });
    };

  });
