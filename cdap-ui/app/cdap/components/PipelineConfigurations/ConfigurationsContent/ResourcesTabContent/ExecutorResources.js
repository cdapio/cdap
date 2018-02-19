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
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import PipelineResources from 'components/PipelineResources';
import {ENGINE_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';

const mapStateToProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    engine: state.engine,
    virtualCores: state.resources.virtualCores,
    memoryMB: state.resources.memoryMB
  };
};
const mapDispatchToProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
      updatePipelineEditStatus();
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
      updatePipelineEditStatus();
    }
  };
};

const ExecutorResources = ({isBatch, engine, virtualCores, onVirtualCoresChange, memoryMB, onMemoryMBChange}) => {
  return (
    <div
      className={classnames("executor", {
        "col-xs-6": isBatch,
        "col-xs-4": !isBatch
      })}
    >
      <span className="resource-title">
        {engine === ENGINE_OPTIONS.MAPREDUCE && isBatch ? 'Mapper/Reducer' : 'Executor'}
      </span>
      <IconSVG
        name="icon-info-circle"
        id="executor-resources-info-icon"
      />
      <UncontrolledTooltip
        target="executor-resources-info-icon"
        delay={{show: 250, hide: 0}}
        placement="right"
      >
        {
          engine === ENGINE_OPTIONS.MAPREDUCE && isBatch ?
            'Resources for the executor process which initializes the pipeline'
          :
            'Resources for executor processes which run tasks in an Apache Spark pipeline'
        }
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

ExecutorResources.propTypes = {
  isBatch: PropTypes.bool,
  engine: PropTypes.string,
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func
};

const ConnectedExecutorResources = connect(mapStateToProps, mapDispatchToProps)(ExecutorResources);

export default ConnectedExecutorResources;
