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
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import PipelineResources from 'components/PipelineResources';
import {getEngineDisplayLabel, ENGINE_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';

const mapStateToStepContentHeadingProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    engine: state.engine
  };
};
const StepContentHeading = ({isBatch, engine}) => {
  return (
    <div className="step-content-heading">
      {`Specify the resources for the following processes of the ${getEngineDisplayLabel(engine, isBatch)} program`}
    </div>
  );
};

StepContentHeading.propTypes = {
  isBatch: PropTypes.bool,
  engine: PropTypes.string
};

const ConnectedStepContentHeading = connect(
  mapStateToStepContentHeadingProps,
  null
)(StepContentHeading);


const mapStateToDriverResourcesProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    virtualCores: state.driverResources.virtualCores,
    memoryMB: state.driverResources.memoryMB
  };
};
const mapDispatchToDriverResourcesProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_DRIVER_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_DRIVER_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
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
      <span className="resource-title">
        Driver
      </span>
      <IconSVG
        name="icon-info-circle"
        id="driver-resources-info-icon"
      />
      <UncontrolledTooltip
        target="driver-resources-info-icon"
        delay={{show: 250, hide: 0}}
        placement="right"
      >
        Resources for the driver process which initializes the pipeline
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

DriverResources.propTypes = {
  isBatch: PropTypes.bool,
  virtualCores: PropTypes.number,
  onVirtualCoresChange: PropTypes.func,
  memoryMB: PropTypes.number,
  onMemoryMBChange: PropTypes.func
};

const ConnectedDriverResources = connect(
  mapStateToDriverResourcesProps,
  mapDispatchToDriverResourcesProps
)(DriverResources);


const mapStateToExecutorResourcesProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    engine: state.engine,
    virtualCores: state.resources.virtualCores,
    memoryMB: state.resources.memoryMB
  };
};
const mapDispatchToExecutorResourcesProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
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

const ConnectedExecutorResources = connect(
  mapStateToExecutorResourcesProps,
  mapDispatchToExecutorResourcesProps
)(ExecutorResources);


const mapStateToClientResourcesProps = (state) => {
  return {
    virtualCores: state.clientResources.virtualCores,
    memoryMB: state.clientResources.memoryMB
  };
};
const mapDispatchToClientResourcesProps = (dispatch) => {
  return {
    onVirtualCoresChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CLIENT_VIRTUAL_CORES,
        payload: { virtualCores: e.target.value }
      });
    },
    onMemoryMBChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CLIENT_MEMORY_MB,
        payload: { memoryMB: e.target.value }
      });
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

const ConnectedClientResources = connect(
  mapStateToClientResourcesProps,
  mapDispatchToClientResourcesProps
)(ClientResources);

export default function ResourcesConfig({isBatch}) {
  return (
    <div className="configuration-step-content resources">
      <ConnectedStepContentHeading
        isBatch={isBatch}
      />
      { !isBatch ? <ConnectedClientResources /> : null }
      <ConnectedDriverResources
        isBatch={isBatch}
      />
      <ConnectedExecutorResources
        isBatch={isBatch}
      />

    </div>
  );
}

ResourcesConfig.propTypes = {
  isBatch: PropTypes.bool
};
