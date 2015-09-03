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

angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetDetailStatusController',
    function($scope, MyDataSource, $state, myHelpers, MyMetricsQueryHelper, myExploreApi, explorableDatasets) {
      this.writes = 0;
      this.reads = 0;
      this.storage = 0;
      this.transactions = 0;
      this.explorable = explorableDatasets;
      if (!explorableDatasets) {
        return;
      }
      var query = myHelpers.objectQuery;
      var dataSrc = new MyDataSource($scope),
          currentDataset = $state.params.datasetId,
          datasetTags = {
            namespace: $state.params.namespace,
            dataset: currentDataset
          };

      [
        {
          name: 'system.dataset.store.reads',
          scopeProperty: 'reads'
        },
        {
          name: 'system.dataset.store.writes',
          scopeProperty: 'writes'
        }
      ].forEach(pollMetric.bind(this));


      function pollMetric(metric) {
        // A temporary way to get the rate of a metric for a dataset.
        // Ideally this would be batched for datasets/streams
        var path = '/metrics/query?metric=' + metric.name +
                    '&' + MyMetricsQueryHelper.tagsToParams(datasetTags) +
                    '&start=now-1s&end=now-1s&resolution=1s';

        dataSrc.poll({
          _cdapPath : path ,
          method: 'POST'
        }, function(metricData) {
          var data = query(metricData, 'series', 0, 'data', 0, 'value');
          this[metric.scopeProperty] = data;
        }.bind(this));
      }

      dataSrc.poll({
        _cdapPath : '/metrics/query?metric=system.dataset.store.bytes' +
                    '&' + MyMetricsQueryHelper.tagsToParams(datasetTags) +
                    '&aggregate=true',
        method: 'POST'
      }, function(metricData) {
        var data = query(metricData, 'series', 0, 'data', 0, 'value');
        this.storage = data;
      }.bind(this));

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');

      var params = {
        namespace: $state.params.namespace,
        table: 'dataset_' + datasetId,
        scope: $scope
      };

      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.schema = query(res, 'schema');
        }.bind(this));

  });
