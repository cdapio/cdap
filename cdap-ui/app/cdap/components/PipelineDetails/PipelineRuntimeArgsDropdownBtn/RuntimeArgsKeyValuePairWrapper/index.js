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
import { updateRunTimeArgs } from 'components/PipelineConfigurations/Store/ActionCreator';
import RuntimeArgsPairs from 'components/PipelineDetails/PipelineRuntimeArgsDropdownBtn/RuntimeArgsKeyValuePairWrapper/RuntimeArgsPairsMaterial';
import { connect } from 'react-redux';
import { getDefaultKeyValuePair } from 'components/KeyValuePairs/KeyValueStore';
import { preventPropagation } from 'services/helpers';

require('./RuntimeArgsKeyValuePairWrapper.scss');

class RuntimeArgsKeyValuePairWrapper extends React.Component {
  runtimeArgsChanged = (changedArgs) => {
    const newArgs = changedArgs.length ? changedArgs : [getDefaultKeyValuePair()];
    updateRunTimeArgs({ pairs: newArgs });
  };

  render() {
    const argsPairs = this.props.runtimeArgs ? this.props.runtimeArgs.pairs : [];
    return (
      <div
        id="runtime-arguments-key-value-pairs-wrapper"
        className="configuration-step-content configuration-content-container"
      >
        <div
          className="runtime-arguments-values key-value-pair-values"
          onClick={preventPropagation}
        >
          <RuntimeArgsPairs
            widgetProps={{
              'key-placeholder': 'Key',
              'value-placeholder': 'Value',
            }}
            onChange={this.runtimeArgsChanged}
            value={argsPairs}
            dataCy="runtimeargs-deployed"
          />
        </div>
      </div>
    );
  }
}

RuntimeArgsKeyValuePairWrapper.propTypes = {
  isHistoricalRun: PropTypes.bool,
  runtimeArgs: PropTypes.object,
};

const mapStateToProps = (state) => {
  return {
    runtimeArgs: state.runtimeArgs,
  };
};

const ConnectedRuntimeArgsKeyValuePairWrapper = connect(mapStateToProps)(
  RuntimeArgsKeyValuePairWrapper
);

export default ConnectedRuntimeArgsKeyValuePairWrapper;
