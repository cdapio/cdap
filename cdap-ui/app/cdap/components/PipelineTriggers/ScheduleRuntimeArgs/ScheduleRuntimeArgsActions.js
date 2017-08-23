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
import NamespaceStore from 'services/NamespaceStore';
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
          let {pluginClass, properties} = ms.spec;
          macros = macros.concat(properties.macros.lookupProperties);
          stagePropertiesMap[ms.id] = Object.keys(pluginClass.properties);
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
function resetStore() {
  ScheduleRuntimeArgsStore.dispatch({
    type: SCHEDULERUNTIMEARGSACTIONS.RESET
  });
}

function setArgMapping(key, value, oldValue) {
  ScheduleRuntimeArgsStore.dispatch({
    type: SCHEDULERUNTIMEARGSACTIONS.SETARGSVALUE,
    payload: {
      mappingKey: key,
      mappingValue: value,
      oldMappedValue: oldValue
    }
  });
}
export {fetchPipelineMacroDetails, setArgMapping, resetStore};
