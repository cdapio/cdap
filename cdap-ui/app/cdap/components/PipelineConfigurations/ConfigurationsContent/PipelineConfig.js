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
import SelectWithOptions from 'components/SelectWithOptions';
import {BATCH_INTERVAL_RANGE, BATCH_INTERVAL_UNITS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';

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

const mapStateToStageLoggingProps = (state) => {
  return {
    stageLogging: state.stageLoggingEnabled
  };
};
const mapDispatchToStageLoggingProps = (dispatch) => {
  return {
    onToggle: (value) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_STAGE_LOGGING,
        payload: { stageLogging: value }
      });
    }
  };
};

const StageLogging = ({stageLogging, onToggle}) => {
  return (
    <div className="label-with-toggle stageLogging row">
      <span className="toggle-label col-xs-4">Stage Level Logging</span>
      <div className="col-xs-7 toggle-container">
        <ToggleSwitch
          isOn={stageLogging}
          onToggle={onToggle.bind(null, !stageLogging)}
        />
        <IconSVG
          name="icon-info-circle"
          id="stagelogging-info-icon"
        />
        <UncontrolledTooltip
          target="stagelogging-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          Allows logs from each stage in the pipeline to be queried individually. It is recommended to always have this setting on, unless the environment is short on resources.
        </UncontrolledTooltip>
      </div>
    </div>
  );
};

StageLogging.propTypes = {
  stageLogging: PropTypes.bool,
  onToggle: PropTypes.func
};

const ConnectedStageLogging = connect(
  mapStateToStageLoggingProps,
  mapDispatchToStageLoggingProps
)(StageLogging);

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
        <IconSVG
          name="icon-info-circle"
          id="checkpointing-info-icon"
        />
        <UncontrolledTooltip
          target="checkpointing-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          Allows Apache Spark Streaming to checkpoint data (RDDs) to persistent storage so that the pipeline can recover from failures.
        </UncontrolledTooltip>
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

const mapStateToSelectBatchIntervalRangeProps = (state) => {
  return {
    value: state.batchInterval.slice(0, -1),
    options: BATCH_INTERVAL_RANGE
  };
};
const mapDispatchToSelectBatchIntervalRangeProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_BATCH_INTERVAL_RANGE,
        payload: { batchIntervalRange: e.target.value }
      });
    }
  };
};

const SelectBatchIntervalRange = connect(
  mapStateToSelectBatchIntervalRangeProps,
  mapDispatchToSelectBatchIntervalRangeProps
)(SelectWithOptions);

const mapStateToSelectBatchIntervalUnitProps = (state) => {
  return {
    value: state.batchInterval.slice(-1),
    options: BATCH_INTERVAL_UNITS
  };
};
const mapDispatchToSelectBatchIntervalUnitProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_BATCH_INTERVAL_UNIT,
        payload: { batchIntervalUnit: e.target.value }
      });
    }
  };
};

const SelectBatchIntervalUnit = connect(
  mapStateToSelectBatchIntervalUnitProps,
  mapDispatchToSelectBatchIntervalUnitProps
)(SelectWithOptions);

const BatchInterval = () => {
  return (
    <div className="label-with-toggle batch-interval form-group row">
      <span className="toggle-label col-xs-4">Batch Interval</span>
      <div className="col-xs-7">
        <SelectBatchIntervalRange className="form-control small-dropdown" />
        <SelectBatchIntervalUnit className="form-control small-dropdown batch-interval-unit" />
      </div>
    </div>
  );
};

export default function PipelineConfig({isBatch}) {
  return (
    <div className="configuration-step-content pipeline-config">
      <div className="step-content-heading">
        Set configurations for this pipeline
      </div>
      {
        !isBatch ?
          (
            <div>
              <ConnectedCheckpointing />
              <BatchInterval />
            </div>
          )
        :
          null
      }
      <ConnectedInstrumentation />
      <ConnectedStageLogging />
    </div>
  );
}

PipelineConfig.propTypes = {
  isBatch: PropTypes.bool
};
