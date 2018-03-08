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
import {connect} from 'react-redux';
import IconSVG from 'components/IconSVG';
import ToggleSwitch from 'components/ToggleSwitch';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';

const mapStateToInstrumentationProps = (state) => {
  return {
    instrumentation: state.processTimingEnabled
  };
};
const mapDispatchToInstrumentationProps = (dispatch) => {
  return {
    onToggle: (value) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_INSTRUMENTATION,
        payload: { instrumentation: value }
      });
      updatePipelineEditStatus();
    }
  };
};

const Instrumentation = ({instrumentation, onToggle}) => {
  return (
    <div className="label-with-toggle instrumentation row">
      <span className="toggle-label col-xs-4">Instrumentation</span>
      <div className="col-xs-7 toggle-container">
        <ToggleSwitch
          isOn={instrumentation}
          onToggle={onToggle.bind(null, !instrumentation)}
        />
        <IconSVG
          name="icon-info-circle"
          id="instrumentation-info-icon"
        />
        <UncontrolledTooltip
          target="instrumentation-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          Emits timing metrics such as total time, mean, standard deviation for pipeline stages. It is recommended to always have this setting on, unless the environment is short on resources.
        </UncontrolledTooltip>
      </div>
    </div>
  );
};

Instrumentation.propTypes = {
  instrumentation: PropTypes.bool,
  onToggle: PropTypes.func
};

const ConnectedInstrumentation = connect(
  mapStateToInstrumentationProps,
  mapDispatchToInstrumentationProps
)(Instrumentation);

export default ConnectedInstrumentation;
