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

import {MyPipelineApi} from 'api/pipeline';
import NamespaceStore, {getCurrentNamespace} from 'services/NamespaceStore';
import ScheduleRuntimeArgsStore, {SCHEDULERUNTIMEARGSACTIONS} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';

function fetchPipelineMacroDetails(pipelineId, namespace, isTriggeredPipeline) {
  if (!namespace) {
    namespace = NamespaceStore.getState().selectedNamespace;
  }
  MyPipelineApi
    .fetchMacros({
      namespace,
      appId: pipelineId
    })
    .subscribe(
      macrosSpec => {
        let macros = [];
        let stagePropertiesMap = {};
        let configStages = [];
        macrosSpec.forEach(ms => {
          let {pluginClass, properties, artifactId: artifact} = ms.spec;
          artifact.version = artifact.version.version;
          macros = macros.concat(properties.macros.lookupProperties);
          stagePropertiesMap[ms.id] = Object.keys(pluginClass.properties);
          updateStagePropertiesFromWidgetJson({
            artifact,
            stageName: ms.name,
            stageType: ms.type,
            id: ms.id
          });
        });
        configStages = Object
          .keys(stagePropertiesMap)
          .map(stage => {
            return {
              id: stage,
              properties: stagePropertiesMap[stage]
            };
          });

        ScheduleRuntimeArgsStore.dispatch({
          type: isTriggeredPipeline ? SCHEDULERUNTIMEARGSACTIONS.SETTRIGGEREDPIPELINEINFO : SCHEDULERUNTIMEARGSACTIONS.SETTRIGGERINGPIPELINEINFO,
          payload: {
            macros,
            configStages,
            id: pipelineId
          }
        });
      }
    );
}
function updateStagePropertiesFromWidgetJson(stage) {
  let {args} = ScheduleRuntimeArgsStore.getState();
  let {stageWidgetJsonMap} = args;
  let {name: artifactName, version: artifactVersion, scope} = stage.artifact;
  let {stageName, stageType, id: stageid} = stage;
  if (!stageWidgetJsonMap[stageid]) {
    MyPipelineApi.fetchWidgetJson({
      namespace: getCurrentNamespace(),
      artifactName,
      artifactVersion,
      scope,
      keys: `widgets.${stageName}-${stageType}`
    })
      .subscribe((stageWidgetJson = {}) => {
        let widgetJson = stageWidgetJson[`widgets.${stageName}-${stageType}`];
        try {
          widgetJson = JSON.parse(widgetJson);
        } catch (e) {
          widgetJson = null;
        }
        if (!widgetJson) {
          return;
        }
        ScheduleRuntimeArgsStore.dispatch({
          type: SCHEDULERUNTIMEARGSACTIONS.SETSTAGEWIDGETJSON,
          payload: {
            stageid,
            stageWidgetJson: widgetJson
          }
        });
      });
  }
}

function resetStore() {
  ScheduleRuntimeArgsStore.dispatch({
    type: SCHEDULERUNTIMEARGSACTIONS.RESET
  });
}

function setArgMapping(key, value, type, oldValue) {
  ScheduleRuntimeArgsStore.dispatch({
    type: SCHEDULERUNTIMEARGSACTIONS.SETARGSVALUE,
    payload: {
      mappingKey: key,
      mappingValue: value,
      type,
      oldMappedValue: oldValue
    }
  });
}

function bulkSetArgMapping(argsArray) {
  ScheduleRuntimeArgsStore.dispatch({
    type: SCHEDULERUNTIMEARGSACTIONS.BULKSETARGSVALUE,
    payload: {
      argsArray
    }
  });
}
export {fetchPipelineMacroDetails, setArgMapping, resetStore, bulkSetArgMapping};
