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

import React, { Component } from 'react';
import {Provider, connect} from 'react-redux';
import PipelineDetailsMetadata from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsMetadata';
import PipelineDetailsButtons from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons';
import PipelineDetailsDetailsActions from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsDetailsActions';
import PipelineDetailStore from 'components/PipelineDetails/store';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {getMacrosResolvedByPrefs} from 'components/PipelineConfigurations/Store/ActionCreator';
import PlusButton from 'components/PlusButton';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {MyPipelineApi} from 'api/pipeline';
import {MyPreferenceApi} from 'api/preference';
import {objectQuery} from 'services/helpers';
import {GLOBALS} from 'services/global-constants';

require('./PipelineDetailsTopPanel.scss');

const mapStateToButtonsProps = (state) => {
  return {
    isBatch: state.artifact.name === GLOBALS.etlDataPipeline,
    pipelineName: state.name,
    schedule: state.config.schedule,
    maxConcurrentRuns: state.config.maxConcurrentRuns,
    scheduleStatus: state.scheduleStatus,
    runs: state.runs,
    currentRun: state.currentRun,
    runButtonLoading: state.runButtonLoading,
    runError: state.runError,
    scheduleButtonLoading: state.scheduleButtonLoading,
    scheduleError: state.scheduleError,
    stopButtonLoading: state.stopButtonLoading,
    stopError: state.stopError
  };
};

const ConnectedPipelineDetailsButtons = connect(mapStateToButtonsProps)(PipelineDetailsButtons);

export default class PipelineDetailsTopPanel extends Component {
  componentWillMount() {
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.INITIALIZE_CONFIG,
      payload: {...PipelineDetailStore.getState().config}
    });
  }
  componentDidMount() {
    const params = {
      namespace: getCurrentNamespace(),
      appId: PipelineDetailStore.getState().name
    };

    MyPipelineApi.fetchMacros(params)
      .combineLatest(MyPreferenceApi.getAppPreferencesResolved(params))
      .subscribe((res) => {
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
        let resolvedMacros = getMacrosResolvedByPrefs(currentAppPrefs, macrosMap);

        PipelineConfigurationsStore.dispatch({
          type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
          payload: { resolvedMacros }
        });
      }, (err) => {
        console.log(err);
      }
    );
  }
  render() {
    return (
      <Provider store={PipelineDetailStore}>
        <div className = "pipeline-details-top-panel">
          <PipelineDetailsMetadata />
          <ConnectedPipelineDetailsButtons />
          <PipelineDetailsDetailsActions />
          <PlusButton mode={PlusButton.MODE.resourcecenter} />
        </div>
      </Provider>
    );
  }
}
