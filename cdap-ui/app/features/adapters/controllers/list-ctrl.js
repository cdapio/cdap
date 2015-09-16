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

var alertpromise;
angular.module(PKG.name + '.feature.adapters')
  .controller('AdaptersListController', function($scope, myAdapterApi, $stateParams, GLOBALS, mySettings, $state, $alert, $timeout, myAlert, myHelpers, myWorkFlowApi, myWorkersApi, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    var vm = this;

    vm.adaptersList = [];
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

    vm.statusMap = {};


    myAdapterApi.list({
      namespace: $stateParams.namespace
    })
      .$promise
      .then(function success(res) {
        vm.adaptersList = res;

        fetchDrafts();

        angular.forEach(vm.adaptersList, function (app) {
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
            if (runs[i].status !== 'RUNNING') {
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
            vm.statusMap[app.appId] = 'Running';
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
                  vm.statusMap[app.appId] = 'Scheduled';
                  vm.statusCount.scheduled++;
                } else {
                  vm.statusMap[app.appId] = 'Suspended';
                  vm.statusCount.suspended++;
                }
              });
          }
        });
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
            vm.statusMap[app.appId] = 'Running';
            vm.statusCount.running++;
          } else {
            vm.statusMap[app.appId] = 'Suspended';
            vm.statusCount.suspended++;
          }
        });
      });

    }

    function fetchDrafts() {
      mySettings.get('adapterDrafts')
        .then(function(res) {
          if (res && Object.keys(res).length) {
            angular.forEach(res, function(value, key) {

              vm.statusCount.draft++;

              vm.adaptersList.push({
                isDraft: true,
                name: key,
                artifact: value.artifact,
                status: '-',
                description: myHelpers.objectQuery(value, 'description'),
                _stats: {
                  numRuns: 'N/A',
                  lastStartTime: 'N/A'
                }
              });

              vm.statusMap[key] = 'Draft';
            });

          }
        });
    }

    vm.deleteDraft = function(draftName) {
      mySettings.get('adapterDrafts')
        .then(function(res) {
          if (res[draftName]) {
            delete res[draftName];
          }
          return mySettings.set('adapterDrafts', res);
        })
        .then(
          function success() {
            var alertObj = {
              type: 'success',
              content: 'Adapter draft ' + draftName + ' deleted successfully'
            }, e;
            if (!alertpromise) {
              alertpromise = $alert(alertObj);
              e = $scope.$on('alert.hide', function() {
                alertpromise = null;
                e(); // un-register from listening to the hide event of a closed alert.
              });
            }
            $state.reload();
          },
          function error() {
            var alertObj = {
              type: 'danger',
              content: 'Adapter draft ' + draftName + ' delete failed'
            }, e;
            if (!alertpromise) {
              alertpromise = $alert(alertObj);
              e = $scope.$on('alert.hide', function() {
                alertpromise = null;
                e();
              });
            }
            $state.reload();
          });
    };
  });
