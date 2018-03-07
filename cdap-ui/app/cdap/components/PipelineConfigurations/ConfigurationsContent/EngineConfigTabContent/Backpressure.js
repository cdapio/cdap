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

const mapStateToProps = (state, ownProps) => {
  return {
    backpressure: state.properties['system.spark.spark.streaming.backpressure.enabled'],
    disabled: ownProps.disabled
  };
};
const mapDispatchToProps = (dispatch) => {
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

const ConnectedBackpressure = connect(mapStateToProps, mapDispatchToProps)(Backpressure);

export default ConnectedBackpressure;
