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
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import {MyArtifactApi} from 'api/artifact';
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import {objectQuery} from 'services/helpers';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import T from 'i18n-react';
import Rx from 'rx';
import find from 'lodash/find';

export default function GetPipelineConfig() {
  let workspaceInfo = DataPrepStore.getState().dataprep.workspaceInfo;
  let namespace = NamespaceStore.getState().selectedNamespace;

  return MyDataPrepApi.getInfo({ namespace })
    .flatMap((res) => {
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
    throw 'Cannot find wrangler-transform plugin. Please load wrangler transform from Cask Market';
  }

  let filteredArtifacts = wranglerArtifacts;

  if (!pluginVersion) {
    let highestVersion = findHighestVersion(wranglerArtifacts.map((artifact) => {
      return artifact.version;
    }), true);

    filteredArtifacts = wranglerArtifacts.filter((artifact) => {
      return artifact.version === highestVersion;
    });
  }

  let returnArtifact = filteredArtifacts[0];

  if (filteredArtifacts.length > 1) {
    returnArtifact.scope = 'USER';
  }

  return returnArtifact;
}

function constructFileSource(artifactsList, properties) {
  if (!properties) { return null; }

  let plugin = objectQuery(properties, 'values', '0');

  let pluginName = Object.keys(plugin)[0];

  plugin = plugin[pluginName];
  let batchArtifact = find(artifactsList, { 'name': 'core-plugins' });
  let realtimeArtifact = find(artifactsList, { 'name': 'spark-plugins' });

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: batchArtifact,
    properties: plugin.properties
  };

  let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
    type: 'streamingsource',
    artifact: realtimeArtifact
  });

  let batchStage = {
    name: 'File',
    plugin: batchPluginInfo
  };

  let realtimeStage = {
    name: 'File',
    plugin: realtimePluginInfo
  };

  return {
    batchSource: batchStage,
    realtimeSource: realtimeStage,
    connections: [{
      from: 'File',
      to: 'Wrangler'
    }]
  };
}

// Need to be modified once backend is complete
function constructDatabaseSource(artifactsList, dbInfo) {
  if (!dbInfo) { return null; }

  let batchArtifact = find(artifactsList, { 'name': 'database-plugins' });
  let pluginName = 'Database';

  try {
    let plugin = objectQuery(dbInfo, 'values', 0, 'Database');

    let pluginInfo = {
      name: 'Database',
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: plugin.properties
    };

    let batchStage = {
      name: pluginName,
      plugin: pluginInfo
    };

    return {
      batchSource: batchStage,
      connections: [{
        from: pluginName,
        to: 'Wrangler'
      }]
    };
  } catch (e) {
    console.log('properties parse error', e);
  }
}

function constructKafkaSource(artifactsList, kafkaInfo) {
  if (!kafkaInfo) { return null; }

  let plugin = objectQuery(kafkaInfo, 'values', '0');
  let pluginName = Object.keys(plugin)[0];

  // This is a hack.. should not do this
  // We are still shipping kafka-plugins with hydrator-plugins 1.7 but
  // it doesn't contain the streamingsource or batchsource plugins
  let pluginArtifact = artifactsList.filter((artifact) => artifact.name === 'kafka-plugins');
  pluginArtifact = pluginArtifact[pluginArtifact.length - 1];

  plugin = plugin[pluginName];

  plugin.properties.schema = {
    name: 'kafkaAvroSchema',
    type: 'record',
    fields: [
      {
        name: 'message',
        type: ['bytes', 'null']
      }
    ]
  };

  let batchPluginInfo = {
    name: plugin.name,
    label: plugin.name,
    type: 'batchsource',
    artifact: pluginArtifact,
    properties: plugin.properties
  };

  let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
    type: 'streamingsource',
    artifact: pluginArtifact
  });

  let batchStage = {
    name: plugin.name,
    plugin: batchPluginInfo
  };

  let realtimeStage = {
    name: plugin.name,
    plugin: realtimePluginInfo
  };

  return {
    batchSource: batchStage,
    realtimeSource: realtimeStage,
    connections: [{
      from: plugin.name,
      to: 'Wrangler'
    }]
  };
}

function constructProperties(workspaceInfo, pluginVersion) {
  let  observable = new Rx.Subject();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let state = DataPrepStore.getState().dataprep;
  let workspaceId = state.workspaceId;

  let requestObj = {
    namespace,
    workspaceId
  };

  let directives = state.directives;

  let requestBody = directiveRequestBodyCreator(directives);

  let rxArray = [
    MyDataPrepApi.getSchema(requestObj, requestBody)
  ];

  if (state.workspaceUri && state.workspaceUri.length > 0) {
    let specParams = {
      namespace,
      path: state.workspaceUri
    };

    rxArray.push(MyDataPrepApi.getSpecification(specParams));
  } else if (state.workspaceInfo.properties.connection === 'database') {
    let specParams = {
      namespace,
      connectionId: state.workspaceInfo.properties.connectionid,
      tableId: state.workspaceInfo.properties.id
    };
    rxArray.push(MyDataPrepApi.getDatabaseSpecification(specParams));
    let requestBody = directiveRequestBodyCreator([]);
    rxArray.push(MyDataPrepApi.getSchema(requestObj, requestBody));
  } else if (state.workspaceInfo.properties.connection === 'kafka') {
    let specParams = {
      namespace,
      connectionId: state.workspaceInfo.properties.connectionid,
      topic: state.workspaceInfo.properties.topic
    };

    rxArray.push(MyDataPrepApi.getKafkaSpecification(specParams));
  }

  try {
    MyArtifactApi.list({ namespace })
    .combineLatest(rxArray)
    .subscribe((res) => {
      let batchArtifact = find(res[0], { 'name': 'cdap-data-pipeline' });
      let realtimeArtifact = find(res[0], { 'name': 'cdap-data-streams' });
      let wranglerArtifact;
      try {
        wranglerArtifact = findWranglerArtifacts(res[0], pluginVersion);
      } catch (e) {
        observable.onError(e);
      }

      let tempSchema = {
        name: 'avroSchema',
        type: 'record',
        fields: res[1]
      };

      let properties = {
        workspaceId,
        directives: directives.join('\n'),
        schema: JSON.stringify(tempSchema),
        field: '*',
        precondition: "false",
        threshold: "1"
      };

      try {
        getParsedSchemaForDataPrep(tempSchema);
      } catch (e) {
        observable.onError(objectQuery(e, 'message'));
      }

      let wranglerStage = {
        name: 'Wrangler',
        plugin: {
          name: 'Wrangler',
          label: 'Wrangler',
          type: 'transform',
          artifact: wranglerArtifact,
          properties
        }
      };

      let connections = [];

      let realtimeStages = [wranglerStage];
      let batchStages = [wranglerStage];

      let sourceConfigs;
      if (state.workspaceInfo.properties.connection === 'file') {
        sourceConfigs = constructFileSource(res[0], res[2]);
      } else if (state.workspaceInfo.properties.connection === 'database') {
        sourceConfigs = constructDatabaseSource(res[0], res[2]);
        sourceConfigs.batchSource.plugin.properties.schema = JSON.stringify({
          name: 'avroSchema',
          type: 'record',
          fields: res[3]
        });
      } else if (state.workspaceInfo.properties.connection === 'kafka') {
        sourceConfigs = constructKafkaSource(res[0], res[2]);
      }

      if (sourceConfigs) {
        realtimeStages.push(sourceConfigs.realtimeSource);
        batchStages.push(sourceConfigs.batchSource);
        connections = sourceConfigs.connections;
      }

      let realtimeConfig = {
        artifact: realtimeArtifact,
        config: {
          stages: realtimeStages,
          batchInterval: '10s',
          connections
        }
      };

      let batchConfig = {
        artifact: batchArtifact,
        config: {
          stages: batchStages,
          connections
        }
      };

      observable.onNext({realtimeConfig, batchConfig});

    }, (err) => {
      observable.onError(objectQuery(err, 'response', 'message')  || T.translate('features.DataPrep.TopPanel.PipelineModal.defaultErrorMessage'));
    });
  } catch (e) {
    observable.onError(objectQuery(e, 'message') || e);
  }
  return observable;
}
