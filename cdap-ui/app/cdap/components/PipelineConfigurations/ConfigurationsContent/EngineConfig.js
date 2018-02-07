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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { Input } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import ToggleSwitch from 'components/ToggleSwitch';
import SelectWithOptions from 'components/SelectWithOptions';
import KeyValuePairs from 'components/KeyValuePairs';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {getEngineDisplayLabel, NUM_EXECUTORS_OPTIONS, ENGINE_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.EngineConfig';

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

const mapStateToCustomConfigProps = (state, ownProps) => {
  return {
    isDetailView: ownProps.isDetailView,
    showCustomConfig: ownProps.showCustomConfig,
    toggleCustomConfig: ownProps.toggleCustomConfig,
    engine: state.engine,
    customConfigKeyValuePairs: state.customConfigKeyValuePairs
  };
};

const mapStateToCustomConfigKeyValuesProps = (state) => {
  return {
    keyValues: state.customConfigKeyValuePairs
  };
};

const mapDispatchToCustomConfigKeyValuesProps = (dispatch) => {
  return {
    onKeyValueChange: (keyValues) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS,
        payload: { keyValues }
      });
    }
  };
};

const ConnectedCustomConfigKeyValuePairs = connect(
  mapStateToCustomConfigKeyValuesProps,
  mapDispatchToCustomConfigKeyValuesProps
)(KeyValuePairs);

const CustomConfig = ({isDetailView, showCustomConfig, toggleCustomConfig, engine, customConfigKeyValuePairs}) => {
  const StudioViewCustomConfigLabel = () => {
    return (
      <span>
        <a
          className="add-custom-config-label"
          onClick={toggleCustomConfig}
        >
          <IconSVG name={showCustomConfig ? "icon-caret-down" : "icon-caret-right"} />
          Show Custom Config
        </a>
        <IconSVG
          name="icon-info-circle"
          id="custom-config-info-icon"
        />
        <UncontrolledTooltip
          target="custom-config-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          {`Enter key-value pairs of configuration parameters that will be passed to the underlying ${getEngineDisplayLabel(engine)} program.`}
        </UncontrolledTooltip>
        {
          showCustomConfig ?
            (
              <span>
                <span className="float-xs-right num-rows">
                  {`${customConfigKeyValuePairs.pairs.length}`}
                  {T.translate(`${PREFIX}.customConfigCount`, {context: customConfigKeyValuePairs.pairs.length})}
                </span>
                <hr />
              </span>
            )
          :
            null
        }
      </span>
    );
  };

  const DetailViewCustomConfigLabel = () => {
    return (
      <span>
        <hr />
        <label>Custom Config</label>
        <IconSVG
          name="icon-info-circle"
          id="custom-config-info-icon"
        />
        <UncontrolledTooltip
          target="custom-config-info-icon"
          delay={{show: 250, hide: 0}}
          placement="right"
        >
          {`Enter key-value pairs of configuration parameters that will be passed to the underlying ${getEngineDisplayLabel(engine)} program.`}
        </UncontrolledTooltip>
        <span className="float-xs-right num-rows">
          {`${customConfigKeyValuePairs.pairs.length} `}
          {T.translate(`${PREFIX}.customConfigCount`, {context: customConfigKeyValuePairs.pairs.length})}
        </span>
      </span>
    );
  };

  return (
    <div className="add-custom-config">
      {
        isDetailView ?
          <DetailViewCustomConfigLabel />
        :
          <StudioViewCustomConfigLabel />
      }
      {
        isDetailView || showCustomConfig ?
          (
            <div>
              <div className="custom-config-labels key-value-pair-labels">
                <span className="key-label">Name</span>
                <span className="value-label">Value</span>
              </div>
              <div className="custom-config-values key-value-pair-values">
                <ConnectedCustomConfigKeyValuePairs />
              </div>
            </div>
          )
        :
          null
      }
    </div>
  );
};

CustomConfig.propTypes = {
  isDetailView: PropTypes.bool,
  showCustomConfig: PropTypes.bool,
  toggleCustomConfig: PropTypes.func,
  engine: PropTypes.string,
  customConfigKeyValuePairs: PropTypes.object
};

const ConnectedCustomConfig = connect(
  mapStateToCustomConfigProps,
  null
)(CustomConfig);

export default class EngineConfig extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    isDetailView: PropTypes.bool
  };

  state = {
    showCustomConfig: false
  };

  toggleCustomConfig = () => {
    this.setState({
      showCustomConfig: !this.state.showCustomConfig
    });
  };

  renderBatchEngineConfig() {
    return (
      <div className="engine-config-radio">
        <label className="radio-inline radio-spark">
          <EngineRadioInput value={ENGINE_OPTIONS.SPARK} />
          Spark
        </label>
        <label className="radio-inline radio-mapReduce">
          <EngineRadioInput value={ENGINE_OPTIONS.MAPREDUCE} />
          MapReduce
        </label>
      </div>
    );
  }

  renderRealtimeEngineConfig(disabled) {
    return (
      <div>
        <ConnectedBackpressure disabled={disabled} />
        <ConnectedNumExecutors />
      </div>
    );
  }

  render() {
    let pipelineTypeLabel = this.props.isBatch ? 'batch' : 'realtime';

    return (
      <div className="configuration-step-content configuration-content-container engine-config">
        <fieldset disabled={this.props.isDetailView}>
          <div className="step-content-heading">
            {`Select the type of engine running your ${pipelineTypeLabel} pipeline`}
          </div>
          {
            this.props.isBatch ?
              this.renderBatchEngineConfig()
            :
              this.renderRealtimeEngineConfig(this.props.isDetailView)
          }
        </fieldset>
        <ConnectedCustomConfig
          isDetailView={this.props.isDetailView}
          showCustomConfig={this.state.showCustomConfig}
          toggleCustomConfig={this.toggleCustomConfig}
        />
      </div>
    );
  }
}
