/*
 * Copyright © 2017 Cask Data, Inc.
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

import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import { directiveRequestBodyCreator } from 'components/DataPrep/helper';
import { MyArtifactApi } from 'api/artifact';
import cdapavsc from 'services/cdapavscwrapper';
import { objectQuery } from 'services/helpers';
import { findHighestVersion } from 'services/VersionRange/VersionUtilities';
import T from 'i18n-react';
import { Subject } from 'rxjs/Subject';
import find from 'lodash/find';
import { SCOPES, HYDRATOR_DEFAULT_VALUES } from 'services/global-constants';

const PREFIX = 'features.DataPrep.PipelineError';

export default function getPipelineConfig() {
  let workspaceInfo = DataPrepStore.getState().dataprep.workspaceInfo;

  return MyDataPrepApi.getInfo().mergeMap((res) => {
    if (res.statusCode === 404) {
      console.log(`can't find method; use latest wrangler-transform`);
      return constructProperties(workspaceInfo);
    }

    let pluginVersion = res.values[0]['plugin.version'];
    return constructProperties(workspaceInfo, pluginVersion);
  });
}

function findWranglerArtifacts(artifacts, pluginVersion) {
  let wranglerArtifacts = artifacts.filter((artifact) => {
    if (pluginVersion) {
      return artifact.name === 'wrangler-transform' && artifact.version === pluginVersion;
    }

    return artifact.name === 'wrangler-transform';
  });

  if (wranglerArtifacts.length === 0) {
    // cannot find plugin. Error out
    throw T.translate(`${PREFIX}.missingWranglerPlugin`);
  }

  let filteredArtifacts = wranglerArtifacts;

  if (!pluginVersion) {
    let highestVersion = findHighestVersion(
      wranglerArtifacts.map((artifact) => {
        return artifact.version;
      }),
      true
    );

    filteredArtifacts = wranglerArtifacts.filter((artifact) => {
      return artifact.version === highestVersion;
    });
  }

  let returnArtifact = filteredArtifacts[0];

  if (filteredArtifacts.length > 1) {
    returnArtifact.scope = SCOPES.USER;
  }

  return returnArtifact;
}

function constructFileSource(artifactsList, properties) {
  if (!properties) {
    return null;
  }

  let plugin = objectQuery(properties, 'values', '0');

  let pluginName = Object.keys(plugin)[0];

  plugin = plugin[pluginName];
  let batchArtifact = find(artifactsList, { name: 'core-plugins' });
  let realtimeArtifact = find(artifactsList, { name: 'spark-plugins' });
  if (!batchArtifact) {
    return T.translate(`${PREFIX}.fileBatch`);
  }

  if (!realtimeArtifact) {
    return T.translate(`${PREFIX}.fileRealtime`);
  }

  batchArtifact.version = '[1.7.0, 3.0.0)';
  realtimeArtifact.version = '[1.7.0, 3.0.0)';

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: plugin.properties,
  };

  let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
    type: 'streamingsource',
    artifact: realtimeArtifact,
  });

  let batchStage = {
    name: 'File',
    plugin: batchPluginInfo,
  };

  let realtimeStage = {
    name: 'File',
    plugin: realtimePluginInfo,
  };

  return {
    batchSource: batchStage,
    realtimeSource: realtimeStage,
    connections: [
      {
        from: 'File',
        to: 'Wrangler',
      },
    ],
  };
}

// Need to be modified once backend is complete
function constructDatabaseSource(artifactsList, dbInfo) {
  if (!dbInfo) {
    return null;
  }

  let batchArtifact = find(artifactsList, { name: 'database-plugins' });

  if (!batchArtifact) {
    return T.translate(`${PREFIX}.database`);
  }
  batchArtifact.version = '[1.7.0, 3.0.0)';
  let pluginName = 'Database';

  try {
    let plugin = objectQuery(dbInfo, 'values', 0, 'Database');

    let pluginInfo = {
      name: 'Database',
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: plugin.properties,
    };

    let batchStage = {
      name: pluginName,
      plugin: pluginInfo,
    };

    return {
      batchSource: batchStage,
      connections: [
        {
          from: pluginName,
          to: 'Wrangler',
        },
      ],
    };
  } catch (e) {
    console.log('properties parse error', e);
  }
}

function constructKafkaSource(artifactsList, kafkaInfo) {
  if (!kafkaInfo) {
    return null;
  }

  let plugin = objectQuery(kafkaInfo, 'values', '0');
  let pluginName = Object.keys(plugin)[0];

  // This is a hack.. should not do this
  // We are still shipping kafka-plugins with hydrator-plugins 1.7 but
  // it doesn't contain the streamingsource or batchsource plugins
  let pluginArtifact = find(artifactsList, { name: 'kafka-plugins' });
  if (!pluginArtifact) {
    return T.translate(`${PREFIX}.kafka`);
  }

  plugin = plugin[pluginName];

  plugin.properties.schema = JSON.stringify({
    name: 'kafkaAvroSchema',
    type: 'record',
    fields: [
      {
        name: 'body',
        type: 'string',
      },
    ],
  });

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: pluginArtifact,
    properties: plugin.properties,
  };

  let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
    type: 'streamingsource',
    artifact: pluginArtifact,
  });

  let batchStage = {
    name: plugin.name,
    plugin: batchPluginInfo,
  };

  let realtimeStage = {
    name: plugin.name,
    plugin: realtimePluginInfo,
  };

  return {
    batchSource: batchStage,
    realtimeSource: realtimeStage,
    connections: [
      {
        from: plugin.name,
        to: 'Wrangler',
      },
    ],
  };
}

function constructS3Source(artifactsList, s3Info) {
  if (!s3Info) {
    return null;
  }
  let batchArtifact = find(artifactsList, { name: 'amazon-s3-plugins' });
  if (!batchArtifact) {
    return T.translate(`${PREFIX}.s3`);
  }
  batchArtifact.version = '[1.7.0, 3.0.0)';
  let plugin = objectQuery(s3Info, 'values', 0, 'S3');
  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: { ...plugin.properties, referenceName: plugin.name },
  };
  let batchStage = {
    name: 'S3',
    plugin: batchPluginInfo,
  };
  return {
    batchSource: batchStage,
    connections: [
      {
        from: 'S3',
        to: 'Wrangler',
      },
    ],
  };
}

function constructGCSSource(artifactsList, gcsInfo) {
  if (!gcsInfo) {
    return null;
  }
  let batchArtifact = find(artifactsList, { name: 'google-cloud' });
  if (!batchArtifact) {
    return T.translate(`${PREFIX}.gcs`);
  }

  batchArtifact.version = '[0.9.0, 3.0.0)';
  let plugin = objectQuery(gcsInfo, 'values', 0);

  let pluginName = Object.keys(plugin)[0]; // this is because the plugin can be GCSFile or GCSFileBlob

  plugin = plugin[pluginName];

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: plugin.properties,
  };

  let batchStage = {
    name: 'GCS',
    plugin: batchPluginInfo,
  };

  return {
    batchSource: batchStage,
    connections: [
      {
        from: 'GCS',
        to: 'Wrangler',
      },
    ],
  };
}

function constructBigQuerySource(artifactsList, bigqueryInfo) {
  if (!bigqueryInfo) {
    return null;
  }

  let batchArtifact = find(artifactsList, { name: 'google-cloud' });
  if (!batchArtifact) {
    return T.translate(`${PREFIX}.bigquery`);
  }

  batchArtifact.version = '[0.9.2, 3.0.0)';
  let plugin = objectQuery(bigqueryInfo, 'values', 0);

  let pluginName = Object.keys(plugin)[0];

  plugin = plugin[pluginName];

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: plugin.properties,
  };

  let batchStage = {
    name: 'BigQueryTable',
    plugin: batchPluginInfo,
  };

  return {
    batchSource: batchStage,
    connections: [
      {
        from: 'BigQueryTable',
        to: 'Wrangler',
      },
    ],
  };
}

function constructSpannerSource(artifactsList, spannerInfo) {
  if (!spannerInfo) {
    return null;
  }

  let googleCloudArtifact = find(artifactsList, { name: 'google-cloud' });
  if (!googleCloudArtifact) {
    return T.translate(`${PREFIX}.spanner`);
  }

  googleCloudArtifact.version = '[0.11.0-SNAPSHOT, 3.0.0)';
  let plugin = objectQuery(spannerInfo, 'values', 0);

  let pluginName = Object.keys(plugin)[0];

  plugin = plugin[pluginName];

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: googleCloudArtifact,
    properties: plugin.properties,
  };

  let batchStage = {
    name: 'SpannerTable',
    plugin: batchPluginInfo,
  };

  return {
    batchSource: batchStage,
    connections: [
      {
        from: 'SpannerTable',
        to: 'Wrangler',
      },
    ],
  };
}

function constructAdlsSource(artifactsList, adlsInfo) {
  if (!adlsInfo) {
    return null;
  }

  let batchArtifact = find(artifactsList, { name: 'adls-plugins' });
  if (!batchArtifact) {
    return T.translate(`${PREFIX}.adls`);
  }

  let plugin = objectQuery(adlsInfo, 'values', 0);

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: plugin.properties,
  };

  let batchStage = {
    name: 'ADLS Batch Source',
    plugin: batchPluginInfo,
  };

  return {
    batchSource: batchStage,
    connections: [
      {
        from: 'ADLS Batch Source',
        to: 'Wrangler',
      },
    ],
  };
}

function constructProperties(workspaceInfo, pluginVersion) {
  let observable = new Subject();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let state = DataPrepStore.getState().dataprep;
  let workspaceId = state.workspaceId;

  let requestObj = {
    context: namespace,
    workspaceId,
  };

  let directives = state.directives;

  let requestBody = directiveRequestBodyCreator(directives);

  let rxArray = [MyDataPrepApi.getSchema(requestObj, requestBody)];
  let connectionId = objectQuery(state, 'workspaceInfo', 'properties', 'connectionid');

  if (state.workspaceInfo.properties.connection === 'file') {
    let specParams = {
      context: namespace,
      path: state.workspaceUri,
      wid: workspaceId,
    };

    rxArray.push(MyDataPrepApi.getSpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'database') {
    let specParams = {
      context: namespace,
      connectionId,
      tableId: state.workspaceInfo.properties.id,
    };
    rxArray.push(MyDataPrepApi.getDatabaseSpecification(specParams));
    let requestBody = directiveRequestBodyCreator([]);
    rxArray.push(MyDataPrepApi.getSchema(requestObj, requestBody));
  } else if (state.workspaceInfo.properties.connection === 'kafka') {
    let specParams = {
      context: namespace,
      connectionId,
      topic: state.workspaceInfo.properties.topic,
    };

    rxArray.push(MyDataPrepApi.getKafkaSpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 's3') {
    let activeBucket = state.workspaceInfo.properties['bucket-name'];
    let key = state.workspaceInfo.properties.key;
    let specParams = {
      context: namespace,
      connectionId,
      activeBucket,
      key,
      wid: workspaceId,
    };
    rxArray.push(MyDataPrepApi.getS3Specification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'gcs') {
    let specParams = {
      context: namespace,
      connectionId: state.workspaceInfo.properties.connectionid,
      wid: workspaceId,
    };
    rxArray.push(MyDataPrepApi.getGCSSpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'bigquery') {
    let specParams = {
      context: namespace,
      connectionId: state.workspaceInfo.properties.connectionid,
      wid: workspaceId,
    };
    rxArray.push(MyDataPrepApi.getBigQuerySpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'spanner') {
    let specParams = {
      context: namespace,
      workspaceId,
    };
    rxArray.push(MyDataPrepApi.getSpannerSpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'adls') {
    let specParams = {
      context: namespace,
      path: state.workspaceUri,
      wid: workspaceId,
      connectionId: state.workspaceInfo.properties.connectionid,
    };
    rxArray.push(MyDataPrepApi.getAdlsSpecification(specParams));
  }

  try {
    MyArtifactApi.list({ namespace })
      .combineLatest(rxArray)
      .subscribe(
        (res) => {
          let batchArtifactsList = res[0].filter((artifact) => {
            return artifact.name === 'cdap-data-pipeline';
          });
          let realtimeArtifactsList = res[0].filter((artifact) => {
            return artifact.name === 'cdap-data-streams';
          });

          let highestBatchArtifactVersion = findHighestVersion(
            batchArtifactsList.map((artifact) => artifact.version),
            true
          );
          let highestRealtimeArtifactVersion = findHighestVersion(
            realtimeArtifactsList.map((artifact) => artifact.version),
            true
          );

          let batchArtifact = {
            name: 'cdap-data-pipeline',
            version: highestBatchArtifactVersion,
            scope: SCOPES.SYSTEM,
          };

          let realtimeArtifact = {
            name: 'cdap-data-streams',
            version: highestRealtimeArtifactVersion,
            scope: SCOPES.SYSTEM,
          };

          let wranglerArtifact;
          try {
            wranglerArtifact = findWranglerArtifacts(res[0], pluginVersion);
          } catch (e) {
            observable.error(e);
          }

          let tempSchema = {
            name: 'avroSchema',
            type: 'record',
            fields: res[1],
          };

          let properties = {
            workspaceId,
            directives: directives.join('\n'),
            schema: JSON.stringify(tempSchema),
            field: '*',
            precondition: 'false',
            threshold: '1',
          };

          if (state.workspaceInfo.properties.connection === 'file') {
            properties.field = 'body';
          }

          try {
            cdapavsc.parse(tempSchema, { wrapUnions: true });
          } catch (e) {
            observable.error(objectQuery(e, 'message'));
          }

          let wranglerStage = {
            name: 'Wrangler',
            plugin: {
              name: 'Wrangler',
              label: 'Wrangler',
              type: 'transform',
              artifact: wranglerArtifact,
              properties,
            },
          };

          let realtimeStages = [wranglerStage];
          let batchStages = [wranglerStage];

          let sourceConfigs, realtimeSource, batchSource;
          let connections = [];
          const connectionType = state.workspaceInfo.properties.connection;
          if (connectionType === 'file') {
            sourceConfigs = constructFileSource(res[0], res[2]);
          } else if (connectionType === 'database') {
            sourceConfigs = constructDatabaseSource(res[0], res[2]);
            delete sourceConfigs.batchSource.plugin.properties.schema;
          } else if (connectionType === 'kafka') {
            sourceConfigs = constructKafkaSource(res[0], res[2]);
          } else if (connectionType === 's3') {
            sourceConfigs = constructS3Source(res[0], res[2]);
          } else if (connectionType === 'gcs') {
            sourceConfigs = constructGCSSource(res[0], res[2]);
          } else if (connectionType === 'bigquery') {
            sourceConfigs = constructBigQuerySource(res[0], res[2]);
          } else if (state.workspaceInfo.properties.connection === 'spanner') {
            sourceConfigs = constructSpannerSource(res[0], res[2]);
          } else if (connectionType === 'adls') {
            sourceConfigs = constructAdlsSource(res[0], res[2]);
          }
          if (typeof sourceConfigs === 'string') {
            observable.error(sourceConfigs);
            return;
          }

          if (sourceConfigs) {
            ({ realtimeSource, batchSource, connections } = sourceConfigs);
          }

          let realtimeConfig = null,
            batchConfig = null;

          if (realtimeSource || connectionType === 'upload') {
            if (realtimeSource) {
              realtimeStages.push(realtimeSource);
            }
            realtimeConfig = {
              artifact: realtimeArtifact,
              config: {
                stages: realtimeStages,
                batchInterval: '10s',
                connections,
                resources: {
                  memoryMB: HYDRATOR_DEFAULT_VALUES.resources.memoryMB,
                  virtualCores: HYDRATOR_DEFAULT_VALUES.resources.virtualCores,
                },
                driverResources: {
                  memoryMB: HYDRATOR_DEFAULT_VALUES.resources.memoryMB,
                  virtualCores: HYDRATOR_DEFAULT_VALUES.resources.virtualCores,
                },
              },
            };
          }

          if (batchSource || connectionType === 'upload') {
            if (batchSource) {
              batchStages.push(batchSource);
            }
            batchConfig = {
              artifact: batchArtifact,
              config: {
                stages: batchStages,
                connections,
                resources: {
                  memoryMB: HYDRATOR_DEFAULT_VALUES.resources.memoryMB,
                  virtualCores: HYDRATOR_DEFAULT_VALUES.resources.virtualCores,
                },
                driverResources: {
                  memoryMB: HYDRATOR_DEFAULT_VALUES.resources.memoryMB,
                  virtualCores: HYDRATOR_DEFAULT_VALUES.resources.virtualCores,
                },
              },
            };
          }

          observable.next({ realtimeConfig, batchConfig });
        },
        (err) => {
          observable.error(
            objectQuery(err, 'response', 'message') || T.translate(`${PREFIX}.defaultMessage`)
          );
        }
      );
  } catch (e) {
    observable.error(objectQuery(e, 'message') || e);
  }
  return observable;
}
