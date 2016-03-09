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
class FlowletDetailDataController {
  constructor($state, $scope, MyCDAPDataSource, myHelpers, MyMetricsQueryHelper, myFlowsApi) {
    this.dataSrc = new MyCDAPDataSource($scope);
    this.$state = $state;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;

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
      .then( (res) => {
        var obj = [];
        var datasets = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'dataSets');

        angular.forEach(datasets, function (v) {
          obj.push({
            name: v
          });
        });

        this.datasets = obj;

        this.pollDatasets();

      });
  }

  pollDatasets() {
    angular.forEach(this.datasets,  (dataset) => {
      var datasetTags = {
        namespace: this.$state.params.namespace,
        dataset: dataset.name,
        app: this.$state.params.appId,
        flow: this.$state.params.programId
      };
      this.dataSrc
        .poll({
          _cdapPath: '/metrics/query?' + this.MyMetricsQueryHelper.tagsToParams(datasetTags) + '&metric=system.dataset.store.reads',
          method: 'POST'
        }, function(res) {
          if (res.series[0]) {
            dataset.reads = res.series[0].data[0].value;
          }
        });

      this.dataSrc
        .poll({
          _cdapPath: '/metrics/query?' + this.MyMetricsQueryHelper.tagsToParams(datasetTags) + '&metric=system.dataset.store.writes',
          method: 'POST'
        }, function(res) {
          if (res.series[0]) {
            dataset.writes = res.series[0].data[0].value;
          }
        });
    });
  }
}

FlowletDetailDataController.$inject = ['$state', '$scope', 'MyCDAPDataSource', 'myHelpers', 'MyMetricsQueryHelper', 'myFlowsApi'];

angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailDataController', FlowletDetailDataController);
