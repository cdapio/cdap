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
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import PipelineResources from 'components/PipelineResources';
import {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.Resources';

const mapStateToProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    virtualCores: state.driverResources.virtualCores,
    memoryMB: state.driverResources.memoryMB
  };
};
const mapDispatchToProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_DRIVER_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
      updatePipelineEditStatus();
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_DRIVER_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
      updatePipelineEditStatus();
    }
  };
};

const DriverResources = ({isBatch, virtualCores, onVirtualCoresChange, memoryMB, onMemoryMBChange}) => {
  return (
    <div
      className={classnames("driver", {
        "col-xs-6": isBatch,
        "col-xs-4": !isBatch
      })}
    >
      <div className="resource-title-icon">
        <span className="resource-title">
          {T.translate(`${PREFIX}.driver`)}
        </span>
        <Popover
          target={() => <IconSVG name="icon-info-circle" />}
          showOn='Hover'
          placement='right'
        >
          {T.translate(`${PREFIX}.driverTooltip`)}
        </Popover>
      </div>
      <PipelineResources
        virtualCores={virtualCores}
        onVirtualCoresChange={onVirtualCoresChange}
        memoryMB={memoryMB}
        onMemoryMBChange={onMemoryMBChange}
      />
    </div>
  );
};

DriverResources.propTypes = {
  isBatch: PropTypes.bool,
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func
};

const ConnectedDriverResources = connect(mapStateToProps, mapDispatchToProps)(DriverResources);

export default ConnectedDriverResources;
