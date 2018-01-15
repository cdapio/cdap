/*
 * Copyright © 2018 Cask Data, Inc.
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

import TriggeredPipelineActions from 'components/TriggeredPipelines/store/TriggeredPipelineActions';
import TriggeredPipelineStore from 'components/TriggeredPipelines/store/TriggeredPipelineStore';
import {MyScheduleApi} from 'api/schedule';
import {MyAppApi} from 'api/app';

export function setTriggeredPipelines(namespace, pipelineName) {
  let params = {
    namespace,
    'trigger-namespace-id': namespace,
    'trigger-program-type': 'workflows',
    'trigger-app-name': pipelineName,
    'trigger-program-name': 'DataPipelineWorkflow',
    'schedule-status': 'SCHEDULED'
  };

  MyScheduleApi.getTriggeredList(params)
    .subscribe((res) => {
      TriggeredPipelineStore.dispatch({
        type: TriggeredPipelineActions.setTriggered,
        payload: {
          triggeredPipelines: res
        }
      });
    });
}

export function togglePipeline(pipeline) {
  TriggeredPipelineStore.dispatch({
    type: TriggeredPipelineActions.setToggle,
    payload: {
      expandedPipeline: pipeline === null ? null : `${pipeline.namespace}_${pipeline.application}`
    }
  });

  if (!pipeline) { return; }

  let params = {
    namespace: pipeline.namespace,
    appId: pipeline.application
  };

  MyAppApi.get(params)
    .subscribe((res) => {
      TriggeredPipelineStore.dispatch({
        type: TriggeredPipelineActions.setPipelineInfo,
        payload: {
          expandedPipelineInfo: res
        }
      });
    });
}
