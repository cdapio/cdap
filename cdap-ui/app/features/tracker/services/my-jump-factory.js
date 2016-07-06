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

function myJumpFactory($state, myTrackerApi, $rootScope) {
  'ngInject';

  let batchArtifact = {
    name: 'cdap-etl-batch',
    scope: 'SYSTEM',
    version: $rootScope.cdapVersion
  };

  let realtimeArtifact = {
    name: 'cdap-etl-realtime',
    scope: 'SYSTEM',
    version: $rootScope.cdapVersion
  };

  let corePluginsArtifacts = {
    name: 'core-plugins',
    scope: 'SYSTEM',
    version: '1.3.4-SNAPSHOT'
  };

  let batchSourceTemplate = {
    artifact: batchArtifact,
    config: {
      sinks: [],
      transforms: [],
      connections: []
    }
  };

  let batchSinkTemplate = {
    artifact: batchArtifact,
    config: {
      source: {},
      transforms: [],
      connections: []
    }
  };

  let realtimeSinkTemplate = {
    artifact: realtimeArtifact,
    config: {
      source: {},
      transforms: [],
      connections: []
    }
  };

  let cdapDatasetTypes = {
    Table: 'co.cask.cdap.api.dataset.table.Table',
    KVTable: 'co.cask.cdap.api.dataset.lib.KeyValueTable'
  };


  function streamBatchSource(streamName) {
    let params = {
      namespace: $state.params.namespace,
      entityId: streamName
    };

    myTrackerApi.getStreamProperties(params)
      .$promise
      .then((res) => {
        let data = angular.copy(batchSourceTemplate);
        data.config.source = {
          name: streamName,
          plugin: {
            name: 'Stream',
            label: streamName,
            artifact: corePluginsArtifacts,
            properties: {
              schema: JSON.stringify(res.format.schema),
              name: streamName,
              format: res.format.name
            },
          }
        };

        $state.go('hydratorplusplus.create', {
          data: data,
          type: 'cdap-etl-batch'
        });
      });
  }

  function streamRealtimeSink(streamName) {
    let data = angular.copy(realtimeSinkTemplate);
    data.config.sinks = [
      {
        name: streamName,
        plugin: {
          name: 'Stream',
          label: streamName,
          artifact: corePluginsArtifacts,
          properties: {
            name: streamName
          }
        }
      }
    ];

    $state.go('hydratorplusplus.create', {
      data: data,
      type: 'cdap-etl-realtime'
    });
  }

  function datasetBatchSource(datasetName) {
    let params = {
      namespace: $state.params.namespace,
      entityType: 'datasets',
      entityId: datasetName
    };
    myTrackerApi.getDatasetSystemProperties(params)
      .$promise
      .then((res) => {
        let datasetType = res.type;

        if (datasetType === cdapDatasetTypes.Table) {
          _addTableBatchSource(datasetName);
        } else if (datasetType === cdapDatasetTypes.KVTable) {
          _addKVTableBatchSource(datasetName);
        }
      });
  }

  function datasetBatchSink(datasetName) {
    let params = {
      namespace: $state.params.namespace,
      entityType: 'datasets',
      entityId: datasetName
    };
    myTrackerApi.getDatasetSystemProperties(params)
      .$promise
      .then((res) => {
        let datasetType = res.type;

        if (datasetType === cdapDatasetTypes.Table) {
          _addTableBatchSink(datasetName);
        } else if (datasetType === cdapDatasetTypes.KVTable) {
          _addKVTableBatchSink(datasetName);
        }
      });
  }

  function isAvailableDataset(datasetType) {
    let available = _.values(cdapDatasetTypes);
    return available.indexOf(datasetType) > -1;
  }

  function _addTableBatchSource(datasetName) {
    let params = {
      namespace: $state.params.namespace,
      entityId: datasetName
    };
    myTrackerApi.getDatasetDetail(params)
      .$promise
      .then((res) => {
        let data = angular.copy(batchSourceTemplate);
        data.config.source = {
          name: datasetName,
          plugin: {
            name: 'Table',
            label: datasetName,
            artifact: corePluginsArtifacts,
            properties: {
              name: datasetName,
              schema: res.spec.properties.schema,
              'schema.row.field': res.spec.properties['schema.row.field']
            }
          }
        };

        $state.go('hydratorplusplus.create', {
          data: data,
          type: 'cdap-etl-batch'
        });
      });
  }

  function _addTableBatchSink(datasetName) {
    let params = {
      namespace: $state.params.namespace,
      entityId: datasetName
    };
    myTrackerApi.getDatasetDetail(params)
      .$promise
      .then((res) => {
        let data = angular.copy(batchSinkTemplate);
        data.config.sinks = [{
          name: datasetName,
          plugin: {
            name: 'Table',
            label: datasetName,
            artifact: corePluginsArtifacts,
            properties: {
              name: datasetName,
              schema: res.spec.properties.schema,
              'schema.row.field': res.spec.properties['schema.row.field']
            }
          }
        }];

        $state.go('hydratorplusplus.create', {
          data: data,
          type: 'cdap-etl-batch'
        });
      });
  }

  function _addKVTableBatchSource(datasetName) {
    let data = angular.copy(batchSourceTemplate);
    data.config.source = {
      name: datasetName,
      plugin: {
        name: 'KVTable',
        label: datasetName,
        artifact: corePluginsArtifacts,
        properties: {
          name: datasetName
        }
      }
    };

    $state.go('hydratorplusplus.create', {
      data: data,
      type: 'cdap-etl-batch'
    });
  }
  function _addKVTableBatchSink(datasetName) {
    let data = angular.copy(batchSinkTemplate);
    data.config.sinks = [{
      name: datasetName,
      plugin: {
        name: 'KVTable',
        label: datasetName,
        artifact: corePluginsArtifacts,
        properties: {
          name: datasetName
        }
      }
    }];

    $state.go('hydratorplusplus.create', {
      data: data,
      type: 'cdap-etl-batch'
    });
  }

  return {
    streamBatchSource: streamBatchSource,
    streamRealtimeSink: streamRealtimeSink,
    datasetBatchSource: datasetBatchSource,
    datasetBatchSink: datasetBatchSink,
    isAvailableDataset: isAvailableDataset
  };

}

angular.module(PKG.name + '.feature.tracker')
  .factory('myJumpFactory', myJumpFactory);
