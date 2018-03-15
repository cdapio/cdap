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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import {convertMapToKeyValuePairsObj} from 'components/KeyValuePairs/KeyValueStoreActions';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {revertRuntimeArgsToSavedValues} from 'components/PipelineConfigurations/Store/ActionCreator';
import PipelineConfigurations from 'components/PipelineConfigurations';
import {objectQuery} from 'services/helpers';
import classnames from 'classnames';

export default class RunConfigs extends Component {
  static propTypes = {
    currentRun: PropTypes.object,
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string
  };

  state = {
    showModeless: false
  };

  getRuntimeArgsAndToggleModeless = () => {
    if (!this.state.showModeless) {
      let runtimeArgs = objectQuery(this.props.currentRun, 'properties', 'runtimeArgs') || '';
      try {
        runtimeArgs = JSON.parse(runtimeArgs);
        delete runtimeArgs['logical.start.time'];
      } catch (e) {
        console.log('ERROR: Cannot parse runtime arguments');
        runtimeArgs = {};
      }

      runtimeArgs = convertMapToKeyValuePairsObj(runtimeArgs);

      PipelineConfigurationsStore.dispatch({
        type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
        payload: { runtimeArgs }
      });
    }

    this.toggleModeless();
  };

  toggleModeless = () => {
    if (this.state.showModeless) {
      revertRuntimeArgsToSavedValues();
    }
    this.setState({
      showModeless: !this.state.showModeless
    });
  };

  renderRunConfigsButton() {
    return (
      <div className="run-configs-btn">
        <IconSVG name="icon-sliders" />
        <div className="button-label">Run Configs</div>
      </div>
    );
  }

  render() {
    return (
      <div
        className={classnames("run-info-container run-configs-container", {"active" : this.state.showModeless})}
        onClick={this.getRuntimeArgsAndToggleModeless}
      >
        {this.renderRunConfigsButton()}
        {
          this.state.showModeless ?
            <PipelineConfigurations
              onClose={this.toggleModeless}
              isDetailView={true}
              isHistoricalRun={true}
              isBatch={this.props.isBatch}
              pipelineName={this.props.pipelineName}
              action="copy"
            />
          :
            null
        }
      </div>
    );
  }
}
