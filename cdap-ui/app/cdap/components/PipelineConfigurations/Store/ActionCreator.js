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

import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import PipelineDetailStore, {ACTIONS as PipelineDetailActions} from 'components/PipelineDetails/store';
import {setRunButtonLoading, setRunError, setScheduleButtonLoading, setScheduleError, fetchScheduleStatus} from 'components/PipelineDetails/store/ActionCreator';
import KeyValueStore, {getDefaultKeyValuePair} from 'components/KeyValuePairs/KeyValueStore';
import KeyValueStoreActions, {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import {GLOBALS} from 'services/global-constants';
import {MyPipelineApi} from 'api/pipeline';
import {MyProgramApi} from 'api/program';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {MyPreferenceApi} from 'api/preference';
import {objectQuery} from 'services/helpers';
import uuidV4 from 'uuid/v4';
import uniqBy from 'lodash/uniqBy';
import cloneDeep from 'lodash/cloneDeep';
import {CLOUD} from 'services/global-constants';


// Filter certain preferences from being shown in the run time arguments
// They are being represented in other places (like selected compute profile).
const getFilteredRuntimeArgs = (runtimeArgs) => {
  const RUNTIME_ARGS_TO_SKIP_DURING_DISPLAY = [
    CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY,
    CLOUD.PROFILE_PROPERTIES_PREFERENCE,
    'logical.start.time'
  ];
  let {resolvedMacros} = PipelineConfigurationsStore.getState();
  let modifiedRuntimeArgs = {};
  let pairs = [...runtimeArgs.pairs];
  const skipIfProfilePropMatch = (prop) => {
    let isMatch = RUNTIME_ARGS_TO_SKIP_DURING_DISPLAY.filter(skipProp => prop.indexOf(skipProp) !== -1);
    return isMatch.length ? true : false;
  };
  pairs = pairs
    .filter(pair => !skipIfProfilePropMatch(pair.key))
    .map(pair => {
      if (pair.key in resolvedMacros) {
        return {
          notDeletable: true,
          provided: pair.provided || false,
          ...pair
        };
      }
      return {
        ...pair,
        // This is needed because KeyValuePair will render a checkbox only if the provided is a boolean.
        provided: null
      };
    });
  if (!pairs.length) {
    pairs.push(getDefaultKeyValuePair());
  }
  modifiedRuntimeArgs.pairs = pairs;
  return modifiedRuntimeArgs;
};

// While adding runtime argument make sure to include the excluded preferences
const updateRunTimeArgs = (rtArgs) => {
  let {runtimeArgs} = PipelineConfigurationsStore.getState();
  let modifiedRuntimeArgs = {};
  let excludedPairs = [...runtimeArgs.pairs];
  const preferencesToFilter = [CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY, CLOUD.PROFILE_PROPERTIES_PREFERENCE];
  const shouldExcludeProperty = (property) => preferencesToFilter.filter(prefProp => property.indexOf(prefProp) !== -1).length;
  excludedPairs = excludedPairs.filter(pair => shouldExcludeProperty(pair.key));
  modifiedRuntimeArgs.pairs = rtArgs.pairs.concat(excludedPairs);
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: {
      runtimeArgs: modifiedRuntimeArgs
    }
  });
};

/*
  FIXME: We should get rid of this function. This is not good. Scenario why this is not good:

  Component 1 uses KeyValuePairs
  Component 1 passes props to KeyValuePairs
  Component 1 receives updates when things chnage via onKeyValuesChange function
  Component 1 now has an update that it needs to pass to KeyValuePairs
  Component 1 updates props -> KeyValuePairs will receive new props -> Updates state
  -> not the store inside it.

  As a result the component doesn't reflect what Component 1 wants. So now Component 1 now
  updates the KeyValueStore as well. This is what this function is doing.

  We should make KeyValuePairs component's API be simpler and easier to use.
*/
const updateKeyValueStore = () => {
  let runtimeArgsPairs = PipelineConfigurationsStore.getState().runtimeArgs.pairs;
  KeyValueStore.dispatch({
    type: KeyValueStoreActions.onUpdate,
    payload: getFilteredRuntimeArgs({ pairs: runtimeArgsPairs })
  });
};

const getMacrosResolvedByPrefs = (resolvedPrefs = {}, macrosMap = {}) => {
  let resolvedMacros = {...macrosMap};
  for (let pref in resolvedPrefs) {
    if (resolvedPrefs.hasOwnProperty(pref) && resolvedMacros.hasOwnProperty(pref)) {
      resolvedMacros[pref] = resolvedPrefs[pref];
    }
  }
  return resolvedMacros;
};

const updatePreferences = () => {
  let {runtimeArgs} = PipelineConfigurationsStore.getState();
  let filteredRuntimeArgs = cloneDeep(runtimeArgs);
  filteredRuntimeArgs.pairs = filteredRuntimeArgs.pairs.filter(runtimeArg => !runtimeArg.provided);
  let appId = PipelineDetailStore.getState().name;
  let prefObj = convertKeyValuePairsObjToMap(runtimeArgs);

  return MyPreferenceApi.setAppPreferences({
    namespace: getCurrentNamespace(),
    appId
  }, prefObj);
};

const updatePipeline = () => {
  let detailStoreState = PipelineDetailStore.getState();
  let { name, description, artifact, principal } = detailStoreState;
  let { stages, connections, comments } = detailStoreState.config;

  let {
    batchInterval,
    engine,
    resources,
    driverResources,
    clientResources,
    postActions,
    properties,
    processTimingEnabled,
    stageLoggingEnabled,
    disableCheckpoints,
    stopGracefully,
    schedule,
    maxConcurrentRuns,
  } = PipelineConfigurationsStore.getState();

  properties = Object.keys(properties)
    .reduce(
      (obj, key) => (obj[key] = properties[key].toString(), obj),
      {}
    );
  let commonConfig = {
    stages,
    connections,
    comments,
    resources,
    driverResources,
    postActions,
    properties,
    processTimingEnabled,
    stageLoggingEnabled
  };

  let batchOnlyConfig = {
    engine,
    schedule,
    maxConcurrentRuns
  };

  let realtimeOnlyConfig = {
    batchInterval,
    clientResources,
    disableCheckpoints,
    stopGracefully
  };

  let config;
  if (artifact.name === GLOBALS.etlDataPipeline) {
    config = {...commonConfig, ...batchOnlyConfig};
  } else {
    config = {...commonConfig, ...realtimeOnlyConfig};
  }

  let publishObservable = MyPipelineApi.publish({
    namespace: getCurrentNamespace(),
    appId: name
  }, {
    name,
    description,
    artifact,
    config,
    principal,
    /*
      Ref: CDAP-13853
      TL;DR - This is here so that when we update the pipeline we don't delete
      the existing schedules (like triggers).

      Longer version:
      The existing behavior was,
      1. User creates a pipeline
      2. Sets up a trigger
      3. Modifies the pipeline engine config

      After step 3 UI was updating the pipeline (PUT /apps/:appId)
      This used to delete any existing schedule as we have deployed/updated
      the app.

      This config will prevent CDAP from deleting existing schedules(triggers)
      when we update the pipeline
    */
    'app.deploy.update.schedules': false
  });

  publishObservable.subscribe(() => {
    PipelineDetailStore.dispatch({
      type: PipelineDetailActions.SET_CONFIG,
      payload: { config }
    });
  });

  return publishObservable;
};

const runPipeline = (runtimeArgs) => {
  setRunButtonLoading(true);
  let { name, artifact } = PipelineDetailStore.getState();

  let params = {
    namespace: getCurrentNamespace(),
    appId: name,
    programType: GLOBALS.programType[artifact.name],
    programId: GLOBALS.programId[artifact.name],
    action: 'start'
  };
  let observerable$ = MyProgramApi.action(params, runtimeArgs);
  observerable$
    .subscribe(
      () => {},
      (err) => {
        setRunButtonLoading(false);
        setRunError(err.response || err);
      });
  return observerable$;
};

const schedulePipeline = () => {
  scheduleOrSuspendPipeline(MyPipelineApi.schedule);
};

const suspendSchedule = () => {
  scheduleOrSuspendPipeline(MyPipelineApi.suspend);
};

const scheduleOrSuspendPipeline = (scheduleApi) => {
  setScheduleButtonLoading(true);
  let { name } = PipelineDetailStore.getState();

  let params = {
    namespace: getCurrentNamespace(),
    appId: name,
    scheduleId: GLOBALS.defaultScheduleId
  };
  scheduleApi(params)
  .subscribe(() => {
    setScheduleButtonLoading(false);
    fetchScheduleStatus(params);
  }, (err) => {
    setScheduleButtonLoading(false);
    setScheduleError(err.response || err);
  });
};

const getCustomizationMap = (properties) => {
  let profileCustomizations = {};
  Object.keys(properties).forEach(prop => {
    if (prop.indexOf(CLOUD.PROFILE_PROPERTIES_PREFERENCE) !== -1) {
      let propName = prop.replace(`${CLOUD.PROFILE_PROPERTIES_PREFERENCE}.`, '');
      profileCustomizations[propName] = properties[prop];
    }
  });
  return profileCustomizations;
};

const fetchAndUpdateRuntimeArgs = () => {
  const params = {
    namespace: getCurrentNamespace(),
    appId: PipelineDetailStore.getState().name
  };

  let observable$ = MyPipelineApi.fetchMacros(params)
    .combineLatest([
      MyPreferenceApi.getAppPreferences(params),
      // This is required to resolve macros from preferences
      // Say DEFAULT_STREAM is a namespace level preference used as a macro
      // in one of the plugins in the pipeline.
      MyPreferenceApi.getAppPreferencesResolved(params)
    ]);

  observable$.subscribe((res) => {
    let macrosSpec = res[0];
    let macrosMap = {};
    let macros = [];
    macrosSpec.map(ms => {
      if (objectQuery(ms, 'spec', 'properties', 'macros', 'lookupProperties')) {
        macros = macros.concat(ms.spec.properties.macros.lookupProperties);
      }
    });
    macros.forEach(macro => {
      macrosMap[macro] = '';
    });

    let currentAppPrefs = res[1];
    let currentAppResolvedPrefs = res[2];
    let resolvedMacros = getMacrosResolvedByPrefs(currentAppResolvedPrefs, macrosMap);
    // When a pipeline is published there won't be any profile related information
    // at app level preference. However the pipeline, when run will be run with the 'default'
    // profile that is set at the namespace level. So we populate in UI the default
    // profile for a pipeline until the user choose something else. This is populated from
    // resolved app level preference which will provide preferences from namespace.
    const isProfileProperty = (property) => (
      [CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY, CLOUD.PROFILE_PROPERTIES_PREFERENCE]
        .filter(profilePrefix => property.indexOf(profilePrefix) !== -1)
        .length
    );
    Object.keys(currentAppResolvedPrefs).forEach(resolvePref => {
      if (isProfileProperty(resolvePref) !== 0) {
        currentAppPrefs[resolvePref] = currentAppResolvedPrefs[resolvePref];
      }
    });

    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
      payload: { resolvedMacros }
    });
    const getPairs = (map) => (
      Object
        .entries(map)
        .filter(([key]) => key.length)
        .map(([key, value]) => ({
          key, value,
          uniqueId: uuidV4()
        }))
    );
    let runtimeArgsPairs = getPairs(currentAppPrefs);
    let resolveMacrosPairs = getPairs(resolvedMacros);
    let finalRunTimeArgsPairs = uniqBy(runtimeArgsPairs.concat(resolveMacrosPairs), (pair) => pair.key);
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
      payload: {
        runtimeArgs: {
          pairs: finalRunTimeArgsPairs
        }
      }
    });
    updateKeyValueStore();
  });
  return observable$;
};

const reset = () => {
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.RESET
  });
};

export {
  getFilteredRuntimeArgs,
  updateRunTimeArgs,
  updateKeyValueStore,
  getMacrosResolvedByPrefs,
  updatePipeline,
  updatePreferences,
  runPipeline,
  schedulePipeline,
  suspendSchedule,
  getCustomizationMap,
  fetchAndUpdateRuntimeArgs,
  reset
};
