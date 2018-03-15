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
import PropTypes from 'prop-types';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updateKeyValueStore} from 'components/PipelineConfigurations/Store/ActionCreator';
import {convertMapToKeyValuePairsObj} from 'components/KeyValuePairs/KeyValueStoreActions';
import RuntimeArgsPairs from 'components/PipelineConfigurations/ConfigurationsContent/RuntimeArgsTabContent/RuntimeArgsPairs';
import ProvidedPopover from 'components/PipelineConfigurations/ConfigurationsContent/RuntimeArgsTabContent/ProvidedPopover';
import classnames from 'classnames';
import isEmpty from 'lodash/isEmpty';
require('./RuntimeArgsTabContent.scss');

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

const onPaste = (dataObj, index) => {
  let runtimeArgs = {...PipelineConfigurationsStore.getState().runtimeArgs};

  // If the selected key-value pair is empty, remove it first before pasting new content
  if (!runtimeArgs.pairs[index].key.length && !runtimeArgs.pairs[index].value.length) {
    runtimeArgs.pairs.splice(index, 1);
  }

  // If there are existing keys, replace the value, and add the remaining
  runtimeArgs.pairs.forEach(runtimeArgsPair => {
    let key = runtimeArgsPair.key;
    if (key in dataObj) {
      runtimeArgsPair.value = dataObj[key];
      delete dataObj[key];
    }
  });
  if (!isEmpty(dataObj)) {
    let remainingRuntimeArgs = convertMapToKeyValuePairsObj(dataObj);
    runtimeArgs.pairs = runtimeArgs.pairs.concat(remainingRuntimeArgs.pairs);
  }
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs }
  });
  updateKeyValueStore();
};

export default function RuntimeArgsTabContent({isHistoricalRun}) {
  let stepContentHeading;
  if (isHistoricalRun) {
    stepContentHeading = (
      <div className="step-content-heading">
        Runtime Arguments used for this run
      </div>
    );
  } else {
    stepContentHeading = (
      <div>
        <div className="step-content-heading">
          Specify Runtime Arguments or Update the Ones Derived from Preferences
        </div>
        <div className="step-content-subtitle">
          By default, values for all runtime arguments must be provided before running the pipeline. If a stage in your pipeline provides the value of an argument, you can skip that argument by marking it as Provided.
        </div>
      </div>
    );
  }

  return (
    <div
      id="runtime-arguments-tab-content"
      className="configuration-step-content configuration-content-container"
    >
      {stepContentHeading}
      <div className="runtime-arguments-labels key-value-pair-labels">
        <span className={classnames("key-label", {"wider": isHistoricalRun})}>
          Name
        </span>
        <span className="value-label">
          Value
        </span>
        <ProvidedPopover
          toggleAllProvided={toggleAllProvided}
          disabled={isHistoricalRun}
        />
      </div>
      <div className="runtime-arguments-values key-value-pair-values">
        <RuntimeArgsPairs
          disabled={isHistoricalRun}
          onPaste={onPaste}
        />
      </div>
    </div>
  );
}

RuntimeArgsTabContent.propTypes = {
  isHistoricalRun: PropTypes.bool
};
