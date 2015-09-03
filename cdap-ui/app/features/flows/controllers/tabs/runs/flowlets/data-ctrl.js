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

angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailDataController', function($state, $scope, MyDataSource, myHelpers, MyMetricsQueryHelper, myFlowsApi) {
    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;
    this.datasets = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {
        var obj = [];
        var datasets = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'dataSets');

        angular.forEach(datasets, function (v) {
          obj.push({
            name: v
          });
        });

        this.datasets = obj;

        pollDatasets.bind(this)();

      }.bind(this));

    function pollDatasets() {
      angular.forEach(this.datasets, function (dataset) {
        var datasetTags = {
          namespace: $state.params.namespace,
          dataset: dataset.name,
          app: $state.params.appId,
          flow: $state.params.programId
        };
        dataSrc
          .poll({
            _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(datasetTags) + '&metric=system.dataset.store.reads',
            method: 'POST'
          }, function(res) {
            if (res.series[0]) {
              dataset.reads = res.series[0].data[0].value;
            }
          });

        dataSrc
          .poll({
            _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(datasetTags) + '&metric=system.dataset.store.writes',
            method: 'POST'
          }, function(res) {
            if (res.series[0]) {
              dataset.writes = res.series[0].data[0].value;
            }
          });
      });
    }

  });
