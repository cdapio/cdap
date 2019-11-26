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
import PipelineConfigurationsStore, {
  ACTIONS as PipelineConfigurationsActions,
} from 'components/PipelineConfigurations/Store';
import { updateKeyValueStore } from 'components/PipelineConfigurations/Store/ActionCreator';
import { convertMapToKeyValuePairs } from 'services/helpers';
import RuntimeArgsPairs from 'components/PipelineDetails/PipelineRuntimeArgsDropdownBtn/RuntimeArgsKeyValuePairWrapper/RuntimeArgsPairs';
import classnames from 'classnames';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
import { connect } from 'react-redux';

require('./RuntimeArgsKeyValuePairWrapper.scss');

const onPaste = (dataObj, index) => {
  let runtimeArgs = { ...PipelineConfigurationsStore.getState().runtimeArgs };

  // If the selected key-value pair is empty, remove it first before pasting new content
  if (!runtimeArgs.pairs[index].key.length && !runtimeArgs.pairs[index].value.length) {
    runtimeArgs.pairs.splice(index, 1);
  }

  // If there are existing keys, replace the value, and add the remaining
  runtimeArgs.pairs.forEach((runtimeArgsPair) => {
    let key = runtimeArgsPair.key;
    if (key in dataObj) {
      runtimeArgsPair.value = dataObj[key];
      delete dataObj[key];
    }
  });
  if (!isEmpty(dataObj)) {
    let remainingRuntimeArgs = convertMapToKeyValuePairs(dataObj);
    runtimeArgs.pairs = runtimeArgs.pairs.concat(remainingRuntimeArgs);
  }
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs },
  });
  updateKeyValueStore();
};

function RuntimeArgsKeyValuePairWrapper({ runtimeArgs }) {
  return (
    <div
      id="runtime-arguments-key-value-pairs-wrapper"
      className="configuration-step-content configuration-content-container"
    >
      <div className={classnames('runtime-arguments-labels key-value-pair-labels')}>
        <span className="key-label">{T.translate('commons.nameLabel')}</span>
        <span className="value-label">{T.translate('commons.keyValPairs.valueLabel')}</span>
      </div>
      <div className="runtime-arguments-values key-value-pair-values">
        <RuntimeArgsPairs onPaste={onPaste} runtimeArgs={runtimeArgs} />
      </div>
    </div>
  );
}

RuntimeArgsKeyValuePairWrapper.propTypes = {
  isHistoricalRun: PropTypes.bool,
  runtimeArgs: PropTypes.object,
};

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: ownProps.runtimeArgs,
  };
};

const ConnectedRuntimeArgsKeyValuePairWrapper = connect(mapStateToProps)(
  RuntimeArgsKeyValuePairWrapper
);

export default ConnectedRuntimeArgsKeyValuePairWrapper;
