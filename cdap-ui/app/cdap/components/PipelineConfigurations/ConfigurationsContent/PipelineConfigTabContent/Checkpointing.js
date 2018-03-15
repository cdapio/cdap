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
import Popover from 'components/Popover';
import {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';

const mapStateToCheckpointingProps = (state) => {
  return {
    checkpointing: state.disableCheckpoints
  };
};
const mapDispatchToCheckpointingProps = (dispatch) => {
  return {
    onToggle: (value) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CHECKPOINTING,
        payload: { checkpointing: value }
      });
      updatePipelineEditStatus();
    }
  };
};

const Checkpointing = ({checkpointing, onToggle}) => {
  return (
    <div className="label-with-toggle checkpointing row">
      <span className="toggle-label col-xs-4">Checkpointing</span>
      <div className="col-xs-7 toggle-container">
        <ToggleSwitch
          isOn={checkpointing}
          onToggle={onToggle.bind(null, !checkpointing)}
        />
        <Popover
          target={() => <IconSVG name="icon-info-circle" />}
          showOn='Hover'
          placement='right'
        >
          Allows Apache Spark Streaming to checkpoint data (RDDs) to persistent storage so that the pipeline can recover from failures.
        </Popover>
      </div>
    </div>
  );
};

Checkpointing.propTypes = {
  checkpointing: PropTypes.bool,
  onToggle: PropTypes.func
};

const ConnectedCheckpointing = connect(
  mapStateToCheckpointingProps,
  mapDispatchToCheckpointingProps
)(Checkpointing);

export default ConnectedCheckpointing;
