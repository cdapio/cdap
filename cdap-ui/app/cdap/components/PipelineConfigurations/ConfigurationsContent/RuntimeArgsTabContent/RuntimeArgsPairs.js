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

import KeyValuePairs from 'components/KeyValuePairs';
import {connect} from 'react-redux';
import {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updateKeyValueStore} from 'components/PipelineConfigurations/Store/ActionCreator';

const mapStateToProps = (state, ownProps) => {
  return {
    keyValues: state.runtimeArgs,
    disabled: ownProps.disabled,
    onPaste: ownProps.onPaste
  };
};

const mapDispatchToProps = (dispatch) => {
  return {
    onKeyValueChange: (keyValues) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
        payload: { runtimeArgs: keyValues }
      });
    },
    getResettedKeyValue: (index) => {
      dispatch({
        type: PipelineConfigurationsActions.RESET_RUNTIME_ARG_TO_RESOLVED_VALUE,
        payload: { index }
      });
      updateKeyValueStore();
    }
  };
};

const ConnectedRuntimeArgsPairs = connect(mapStateToProps, mapDispatchToProps)(KeyValuePairs);

export default ConnectedRuntimeArgsPairs;
