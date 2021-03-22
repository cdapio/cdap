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

import PipelineTriggersActions from 'components/PipelineTriggers/store/PipelineTriggersActions';
import PipelineTriggersStore from 'components/PipelineTriggers/store/PipelineTriggersStore';
import NamespaceStore from 'services/NamespaceStore';
import { MyAppApi } from 'api/app';
import { MyScheduleApi } from 'api/schedule';
import { GLOBALS } from 'services/global-constants';
import { extractErrorMessage } from 'services/helpers';

const WORKFLOW_TYPE = 'workflows';

export function changeNamespace(namespace) {
  let currentNamespace = NamespaceStore.getState().selectedNamespace;
  let state = PipelineTriggersStore.getState().triggers;
  let pipelineName = state.pipelineName;
  let existingTriggers = state.enabledTriggers;

  MyAppApi.list({
    namespace: namespace,
  }).subscribe((res) => {
    let pipelineList = _filterPipelineList(
      existingTriggers,
      res,
      currentNamespace,
      namespace,
      pipelineName
    );

    PipelineTriggersStore.dispatch({
      type: PipelineTriggersActions.changeNamespace,
      payload: {
        pipelineList,
        selectedNamespace: namespace,
      },
    });
  });
}

function setConfigureError(err) {
  console.log('Error configuring trigger', err);
  const errMessage = extractErrorMessage(err);
  PipelineTriggersStore.dispatch({
    type: PipelineTriggersActions.setConfigureTriggerError,
    payload: {
      error: errMessage,
    },
  });
}

export function enableSchedule(
  pipelineTrigger,
  workflowName,
  activePipeline,
  selectedNamespace,
  config
) {
  let namespace = NamespaceStore.getState().selectedNamespace;
  let scheduleName = `${activePipeline}.${namespace}.${pipelineTrigger.id}.${selectedNamespace}`;

  let requestObj = {
    name: scheduleName,
    description: '',
    program: {
      programName: workflowName,
      programType: 'WORKFLOW',
    },
    properties: Object.assign({}, config.properties),
    trigger: {
      programId: {
        namespace: selectedNamespace,
        application: pipelineTrigger.id,
        version: pipelineTrigger.version || '-SNAPSHOT', // FIXME: This is a temporary hack and is not required
        type: 'WORKFLOW',
        entity: 'PROGRAM',
        program: pipelineTrigger.workflowName,
      },
      programStatuses: config.eventTriggers,
      type: 'PROGRAM_STATUS',
    },
    constraints: [
      {
        maxConcurrency: 3,
        type: 'CONCURRENCY',
        waitUntilMet: false,
      },
    ],
    timeoutMillis: 86400000,
  };

  // This API change will be replaced with a single API from Backend
  let scheduleParams = {
    namespace,
    appId: activePipeline,
    scheduleName,
  };

  MyScheduleApi.get(scheduleParams).subscribe(
    () => {
      // Schedule exist, update it
      MyScheduleApi.update(scheduleParams, requestObj)
        .flatMap(() => {
          return MyScheduleApi.enableTrigger(scheduleParams);
        })
        .subscribe(() => {
          // fetch list triggers
          fetchTriggersAndApps(activePipeline, workflowName);
        }, setConfigureError);
    },
    () => {
      // Schedule does not exist, create it
      MyScheduleApi.create(scheduleParams, requestObj)
        .flatMap(() => {
          return MyScheduleApi.enableTrigger(scheduleParams);
        })
        .subscribe(() => {
          // fetch list triggers
          fetchTriggersAndApps(activePipeline, workflowName);
        }, setConfigureError);
    }
  );
}

export function fetchTriggersAndApps(pipeline, workflowName, activeNamespace) {
  let namespace = NamespaceStore.getState().selectedNamespace;
  let activeNamespaceView =
    activeNamespace || PipelineTriggersStore.getState().triggers.selectedNamespace;

  let params = {
    namespace,
    appId: pipeline,
    workflowId: workflowName,
    'trigger-type': 'program-status',
    'schedule-status': 'SCHEDULED',
  };

  MyScheduleApi.getTriggers(params)
    .combineLatest(MyAppApi.list({ namespace: activeNamespaceView }))
    .subscribe((res) => {
      let existingTriggers = res[0];
      let appsList = res[1];

      let pipelineList = _filterPipelineList(
        existingTriggers,
        appsList,
        namespace,
        activeNamespaceView,
        pipeline
      );

      PipelineTriggersStore.dispatch({
        type: PipelineTriggersActions.setTriggersAndPipelineList,
        payload: {
          pipelineList,
          enabledTriggers: existingTriggers,
          selectedNamespace: activeNamespaceView,
        },
      });
    });
}

export function disableSchedule(schedule, activePipeline, workflowName) {
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    namespace,
    appId: activePipeline,
    scheduleName: schedule.name,
  };

  MyScheduleApi.delete(params).subscribe(
    () => {
      fetchTriggersAndApps(activePipeline, workflowName);
    },
    (err) => {
      console.log('Error deleting trigger', err);
      const errMessage = extractErrorMessage(err);
      PipelineTriggersStore.dispatch({
        type: PipelineTriggersActions.setDisableTriggerError,
        payload: {
          error: errMessage,
        },
      });
    }
  );
}

export function getPipelineInfo(schedule) {
  PipelineTriggersStore.dispatch({
    type: PipelineTriggersActions.setExpandedTrigger,
    payload: {
      expandedTrigger: schedule ? schedule.name : null,
    },
  });

  if (!schedule) {
    return;
  }

  let params = {
    namespace: schedule.trigger.programId.namespace,
    appId: schedule.trigger.programId.application,
  };

  MyAppApi.get(params).subscribe((res) => {
    PipelineTriggersStore.dispatch({
      type: PipelineTriggersActions.setEnabledTriggerPipelineInfo,
      payload: {
        pipelineInfo: res,
      },
    });
  });
}

function _filterPipelineList(
  existingTriggers,
  appsList,
  namespace,
  activeNamespaceView,
  activePipeline
) {
  let triggersPipelineName = existingTriggers
    .filter((schedule) => {
      return schedule.trigger.programId.namespace === activeNamespaceView;
    })
    .map((schedule) => {
      return schedule.trigger.programId.application;
    });

  let pipelineList = appsList.filter((app) => {
    let isWorkflow = GLOBALS.programType[app.artifact.name] === WORKFLOW_TYPE;

    let isCurrentNamespace = namespace === activeNamespaceView ? app.name !== activePipeline : true;

    let isExistingTrigger = triggersPipelineName.indexOf(app.name) === -1;

    return isWorkflow && isCurrentNamespace && isExistingTrigger;
  });

  return pipelineList;
}
