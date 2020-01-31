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
import { connect } from 'react-redux';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import PipelineResources from 'components/PipelineResources';
import { ENGINE_OPTIONS } from 'components/PipelineConfigurations/PipelineConfigConstants';
import { ACTIONS as PipelineConfigurationsActions } from 'components/PipelineConfigurations/Store';
import T from 'i18n-react';
import { GLOBALS } from 'services/global-constants';

const PREFIX = 'features.PipelineConfigurations.Resources';

const mapStateToProps = (state, ownProps) => {
  return {
    pipelineType: ownProps.pipelineType,
    engine: state.engine,
    virtualCores: state.resources.virtualCores,
    memoryMB: state.resources.memoryMB,
  };
};
const mapDispatchToProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value },
      });
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_MB,
        payload: { memoryMB: e.target.value },
      });
    },
  };
};

const ExecutorResources = ({
  pipelineType,
  engine,
  virtualCores,
  onVirtualCoresChange,
  memoryMB,
  onMemoryMBChange,
}) => {
  const isMapReduce =
    engine === ENGINE_OPTIONS.MAPREDUCE && GLOBALS.etlBatchPipelines.includes(pipelineType);
  return (
    <div className="executor">
      <div className="resource-title-icon">
        <span className="resource-title">
          {isMapReduce
            ? T.translate(`${PREFIX}.executorMapReduce`)
            : T.translate(`${PREFIX}.executor`)}
        </span>
        <Popover
          target={() => <IconSVG name="icon-info-circle" />}
          showOn="Hover"
          placement="right"
        >
          {isMapReduce
            ? T.translate(`${PREFIX}.executorMapReduceTooltip`)
            : T.translate(`${PREFIX}.executorTooltip`)}
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

ExecutorResources.propTypes = {
  pipelineType: PropTypes.string,
  engine: PropTypes.string,
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func,
};

const ConnectedExecutorResources = connect(mapStateToProps, mapDispatchToProps)(ExecutorResources);

export default ConnectedExecutorResources;
