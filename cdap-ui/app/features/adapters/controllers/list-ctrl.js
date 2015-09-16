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
  .controller('AdaptersListController', function($scope, myAdapterApi, $stateParams, GLOBALS, mySettings, $state, $alert, $timeout, myAlert, myHelpers, myWorkFlowApi) {

    var vm = this;

    vm.adaptersList = [];
    vm.drafts  = [];
    vm.currentPage = 1;
    vm.searchText = '';

    vm.GLOBALS = GLOBALS;


    myAdapterApi.list({
      namespace: $stateParams.namespace
    })
      .$promise
      .then(function success(res) {
        vm.adaptersList = res;

        angular.forEach(vm.adaptersList, function (app) {
          app._stats = {};

          fetchRunsInfo(app);

        });


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
      }
      // else {
      //   api = myWorkersApi;
      //   params.workerId = 'ETLWorker';
      // }

      api.runs(params)
        .$promise
        .then(function (runs) {
          app._stats.numRuns = runs.length;
          console.log('runs', runs);
          app._stats.lastStartTime = runs.length > 0 ? runs[0].start : 'N/A';
        });

    }



      // DRAFTS

    mySettings.get('adapterDrafts')
      .then(function(res) {
        if (res && Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            this.drafts.push({
              isdraft: true,
              name: key,
              template: myHelpers.objectQuery(value, 'artifact', 'name'),
              status: '-',
              description: myHelpers.objectQuery(value, 'description')
            });
          });
        }
      });

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
              content: 'Adapter draft ' + draftName + ' delete successfully'
            }, e;
            if (!alertpromise) {
              alertpromise = $alert(alertObj);
              e = this.$on('alert.hide', function() {
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
              e = this.$on('alert.hide', function() {
                alertpromise = null;
                e();
              });
            }
            $state.reload();
          });
    };
  });
