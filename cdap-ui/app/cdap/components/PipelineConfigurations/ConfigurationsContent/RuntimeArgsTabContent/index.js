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

import React from 'react';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import KeyValueStore from 'components/KeyValuePairs/KeyValueStore';
import KeyValueStoreActions from 'components/KeyValuePairs/KeyValueStoreActions';
import RuntimeArgsPairs from 'components/PipelineConfigurations/ConfigurationsContent/RuntimeArgsTabContent/RuntimeArgsPairs';
import ProvidedPopover from 'components/PipelineConfigurations/ConfigurationsContent/RuntimeArgsTabContent/ProvidedPopover';
require('./RuntimeArgsTabContent.scss');

const updateKeyValueStore = () => {
  let runtimeArgsPairs = PipelineConfigurationsStore.getState().runtimeArgs.pairs;
  KeyValueStore.dispatch({
    type: KeyValueStoreActions.onUpdate,
    payload: { pairs: runtimeArgsPairs }
  });
};

const toggleAllProvided = (isProvided) => {
  let runtimeArgs = {...PipelineConfigurationsStore.getState().runtimeArgs};
  runtimeArgs.pairs.forEach(runtimeArgsPair => {
    if (runtimeArgsPair.notDeletable) {
      runtimeArgsPair.provided = isProvided;
    }
  });
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs }
  });
  updateKeyValueStore();
};

export default function RuntimeArgsTabContent() {
  return (
    <div
      id="runtime-arguments-tab-content"
      className="configuration-step-content configuration-content-container"
    >
      <div className="step-content-heading">
        Specify Runtime Arguments or Update the Ones Derived from Preferences
        <div className="step-content-subtitle">
          By default, values for all runtime arguments must be provided before running the pipeline. If a stage in your pipeline provides the value of an argument, you can skip that argument by marking it as Provided.
        </div>
      </div>
      <div className="runtime-arguments-labels key-value-pair-labels">
        <span className="key-label">
          Name
        </span>
        <span className="value-label">
          Value
        </span>
        <ProvidedPopover
          toggleAllProvided={toggleAllProvided}
        />
      </div>
      <div className="runtime-arguments-values key-value-pair-values">
        <RuntimeArgsPairs
          updateKeyValueStore={updateKeyValueStore}
        />
      </div>
    </div>
  );
}
