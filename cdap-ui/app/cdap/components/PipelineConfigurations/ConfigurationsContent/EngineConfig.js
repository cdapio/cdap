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
import { Input } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import ToggleSwitch from 'components/ToggleSwitch';
import SelectWithOptions from 'components/SelectWithOptions';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {NUM_EXECUTORS_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';

const mapStateToEngineInputProps = (state, ownProps) => {
  return {
    type: 'radio',
    checked: state.engine === ownProps.value,
    value: ownProps.value
  };
};

const mapDispatchToEngineInputProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_ENGINE,
        payload: { engine: e.target.value }
      });
    }
  };
};

const EngineRadioInput = connect(
  mapStateToEngineInputProps,
  mapDispatchToEngineInputProps
)(Input);

const renderBatchEngineConfig = () => {
  return (
    <div className="engine-config-radio">
      <label className="radio-inline radio-spark">
        <EngineRadioInput value="spark" />
        Spark
      </label>
      <label className="radio-inline radio-mapReduce">
        <EngineRadioInput value="mapreduce" />
        MapReduce
      </label>
    </div>
  );
};

const mapStateToBackpressureProps = (state, ownProps) => {
  return {
    backpressure: state.properties['system.spark.spark.streaming.backpressure.enabled'],
    disabled: ownProps.disabled
  };
};
const mapDispatchToBackpressureProps = (dispatch) => {
  return {
    onToggle: (value) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_BACKPRESSURE,
        payload: { backpressure: value }
      });
    }
  };
};

const Backpressure = ({backpressure, disabled, onToggle}) => {
  return (
    <div className="label-with-toggle backpressure row">
      <span className="toggle-label col-xs-4">Backpressure</span>
      <div className="col-xs-7 toggle-container">
        <ToggleSwitch
          isOn={backpressure}
          onToggle={onToggle.bind(null, !backpressure)}
          disabled={disabled}
        />
        <IconSVG
          name="icon-info-circle"
          id="backpressure-info-icon"
        />
        <UncontrolledTooltip
          target="backpressure-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          Allows the Apache Spark Streaming engine to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.
        </UncontrolledTooltip>
      </div>
    </div>
  );
};

Backpressure.propTypes = {
  backpressure: PropTypes.string,
  disabled: PropTypes.bool,
  onToggle: PropTypes.func
};

const ConnectedBackpressure = connect(
  mapStateToBackpressureProps,
  mapDispatchToBackpressureProps
)(Backpressure);


const mapStateToNumExecutorsProps = (state) => {
  let numExecutorsKeyName = window.CDAP_CONFIG.isEnterprise ? 'system.spark.spark.executor.instances' : 'system.spark.spark.master';
  return {
    numExecutors: state.properties[numExecutorsKeyName]
  };
};
const mapDispatchToNumExecutorsProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_NUM_EXECUTORS,
        payload: { numExecutors: e.target.value }
      });
    }
  };
};

const NumExecutors = ({numExecutors, onChange}) => {
  return (
    <div className="label-with-toggle numExecutors form-group row">
      <span className="toggle-label col-xs-4">Number of Executors</span>
      <div className="col-xs-7">
        <SelectWithOptions
          className="form-control small-dropdown"
          value={numExecutors}
          options={NUM_EXECUTORS_OPTIONS}
          onChange={onChange}
        />
        <IconSVG
          name="icon-info-circle"
          id="num-executors-info-icon"
        />
        <UncontrolledTooltip
          target="num-executors-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          The number of executors to allocate for this pipeline on Apache Yarn.
        </UncontrolledTooltip>
      </div>
    </div>
  );
};

NumExecutors.propTypes = {
  numExecutors: PropTypes.string,
  onChange: PropTypes.func
};

const ConnectedNumExecutors = connect(
  mapStateToNumExecutorsProps,
  mapDispatchToNumExecutorsProps
)(NumExecutors);


const renderRealtimeEngineConfig = (disabled) => {
  return (
    <div>
      <ConnectedBackpressure disabled={disabled} />
      <ConnectedNumExecutors />
    </div>
  );
};

export default function EngineConfig({isBatch, isDetailView}) {
  let pipelineTypeLabel = isBatch ? 'batch' : 'realtime';

  return (
    <div className="configuration-step-content configuration-content-container engine-config">
      <fieldset disabled={isDetailView}>
        <div className="step-content-heading">
          {`Select the type of engine running your ${pipelineTypeLabel} pipeline`}
        </div>
        {
          isBatch ?
            renderBatchEngineConfig()
          :
            renderRealtimeEngineConfig(isDetailView)
        }
      </fieldset>
    </div>
  );
}

EngineConfig.propTypes = {
  isBatch: PropTypes.bool,
  isDetailView: PropTypes.bool
};
