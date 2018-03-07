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
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import PipelineResources from 'components/PipelineResources';
import {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';

const mapStateToProps = (state) => {
  return {
    virtualCores: state.clientResources.virtualCores,
    memoryMB: state.clientResources.memoryMB
  };
};
const mapDispatchToProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CLIENT_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
      updatePipelineEditStatus();
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CLIENT_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
      updatePipelineEditStatus();
    }
  };
};

const ClientResources = ({virtualCores, onVirtualCoresChange, memoryMB, onMemoryMBChange}) => {
  return (
    <div className="col-xs-4 client">
      <span className="resource-title">
        Client
      </span>
      <IconSVG
        name="icon-info-circle"
        id="client-resources-info-icon"
      />
      <UncontrolledTooltip
        target="client-resources-info-icon"
        delay={{show: 250, hide: 0}}
        placement="right"
      >
        Resources for the client process which launches the Apache Spark Streaming pipeline
      </UncontrolledTooltip>
      <PipelineResources
        virtualCores={virtualCores}
        onVirtualCoresChange={onVirtualCoresChange}
        memoryMB={memoryMB}
        onMemoryMBChange={onMemoryMBChange}
      />
    </div>
  );
};

ClientResources.propTypes = {
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func
};

const ConnectedClientResources = connect(mapStateToProps, mapDispatchToProps)(ClientResources);

export default ConnectedClientResources;
