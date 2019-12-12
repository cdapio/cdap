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
import {ENGINE_OPTIONS} from 'components/PipelineConfigurations/PipelineConfigConstants';
import EngineRadioInput from 'components/PipelineConfigurations/ConfigurationsContent/EngineConfigTabContent/EngineRadioInput';
import Backpressure from 'components/PipelineConfigurations/ConfigurationsContent/EngineConfigTabContent/Backpressure';
import NumExecutors from 'components/PipelineConfigurations/ConfigurationsContent/EngineConfigTabContent/NumExecutors';
import CustomConfig from 'components/PipelineConfigurations/ConfigurationsContent/EngineConfigTabContent/CustomConfig';
import {connect} from 'react-redux';
import T from 'i18n-react';
import classnames from 'classnames';
require('./EngineConfigTabContent.scss');

const PREFIX = 'features.PipelineConfigurations.EngineConfig';

class EngineConfigTabContent extends Component {
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
          {T.translate('commons.entity.spark.singular')}
        </label>
        <label className="radio-inline radio-mapReduce">
          <EngineRadioInput value={ENGINE_OPTIONS.MAPREDUCE} />
          {T.translate('commons.entity.mapreduce.singular')}
        </label>
      </div>
    );
  }

  renderRealtimeEngineConfig(disabled) {
    return (
      <div>
        <Backpressure disabled={disabled} />
        <NumExecutors />
      </div>
    );
  }

  render() {
    let pipelineTypeLabel = this.props.isBatch ? 'batch' : 'realtime';

    return (
      <div
        id="engine-config-tab-content"
        className={classnames(
          "configuration-step-content configuration-content-container", {
            "batch-content": this.props.isBatch,
            "realtime-content": !this.props.isBatch
          }
        )}
      >
        {/*
          currenlty only spark type of engine running and map-reduce not working so hiding
          renderBatchEngineConfig()
        */}
        {
          this.props.isBatch ? null :
            <fieldset disabled={this.props.isDetailView}>
              <div className="step-content-heading">
                {T.translate(`${PREFIX}.contentHeading`, {pipelineTypeLabel})}
              </div>
              { this.renderRealtimeEngineConfig(this.props.isDetailView) }
            </fieldset>
        }
        <CustomConfig
          isDetailView={this.props.isDetailView}
          showCustomConfig={this.state.showCustomConfig}
          toggleCustomConfig={this.toggleCustomConfig}
          isBatch={this.props.isBatch}
        />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    isBatch: state.pipelineVisualConfiguration.isBatch,
    isDetailView: state.pipelineVisualConfiguration.isDetailView
  };
};

const ConnectedEngineConfigTabContent = connect(mapStateToProps)(EngineConfigTabContent);

export default ConnectedEngineConfigTabContent;
