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

  function streamBatchSource(streamName) {
    let params = {
      namespace: $state.params.namespace,
      entityId: streamName
    };

    myTrackerApi.getStreamProperties(params)
      .$promise
      .then((res) => {
        let data = {
          artifact: batchArtifact,
          config: {
            source: {
              name: streamName,
              plugin: {
                name: 'Stream',
                label: streamName,
                artifact: {
                  name: 'core-plugins',
                  scope: 'SYSTEM',
                  version: '1.3.0-SNAPSHOT'
                },
                properties: {
                  schema: JSON.stringify(res.format.schema),
                  name: streamName,
                  format: res.format.name
                },
              }
            },
            sinks: [],
            transforms: [],
            connections: []
          }
        };

        $state.go('hydrator.create.studio', {
          data: data,
          type: 'cdap-etl-batch'
        });
      });
  }

  function streamRealtimeSink(streamName) {
    let data = {
      artifact: realtimeArtifact,
      config: {
        source: {},
        transforms: [],
        sinks: [
          {
            name: streamName,
            plugin: {
              name: 'Stream',
              label: streamName,
              artifact: {
                name: 'core-plugins',
                scope: 'SYSTEM',
                version: '1.3.0-SNAPSHOT'
              },
              properties: {
                name: streamName
              }
            }
          }
        ],
        connections: []
      }
    };

    $state.go('hydrator.create.studio', {
      data: data,
      type: 'cdap-etl-realtime'
    });
  }


  return {
    streamBatchSource: streamBatchSource,
    streamRealtimeSink: streamRealtimeSink
  };

}

angular.module(PKG.name + '.feature.tracker')
  .factory('myJumpFactory', myJumpFactory);
