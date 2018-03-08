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
import KeyValuePairs from 'components/KeyValuePairs';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {getEngineDisplayLabel, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.EngineConfig';

const mapStateToCustomConfigKeyValuesProps = (state) => {
  return {
    keyValues: state.customConfigKeyValuePairs
  };
};

const mapDispatchToCustomConfigKeyValuesProps = (dispatch, ownProps) => {
  return {
    onKeyValueChange: (keyValues) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS,
        payload: { keyValues }
      });
      let customConfigObj = convertKeyValuePairsObjToMap(keyValues);
      dispatch({
        type: PipelineConfigurationsActions.SET_CUSTOM_CONFIG,
        payload: {
          customConfig: customConfigObj,
          isBatch: ownProps.isBatch
        }
      });
      updatePipelineEditStatus();
    }
  };
};

const ConnectedCustomConfigKeyValuePairs = connect(
  mapStateToCustomConfigKeyValuesProps,
  mapDispatchToCustomConfigKeyValuesProps
)(KeyValuePairs);

const mapStateToCustomConfigProps = (state, ownProps) => {
  return {
    isDetailView: ownProps.isDetailView,
    isBatch: ownProps.isBatch,
    showCustomConfig: ownProps.showCustomConfig,
    toggleCustomConfig: ownProps.toggleCustomConfig,
    engine: state.engine,
    customConfigKeyValuePairs: state.customConfigKeyValuePairs
  };
};

const CustomConfig = ({isDetailView, isBatch, showCustomConfig, toggleCustomConfig, engine, customConfigKeyValuePairs}) => {
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
                <ConnectedCustomConfigKeyValuePairs isBatch={isBatch} />
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
  isBatch: PropTypes.bool,
  showCustomConfig: PropTypes.bool,
  toggleCustomConfig: PropTypes.func,
  engine: PropTypes.string,
  customConfigKeyValuePairs: PropTypes.object
};

const ConnectedCustomConfig = connect(mapStateToCustomConfigProps)(CustomConfig);

export default ConnectedCustomConfig;
