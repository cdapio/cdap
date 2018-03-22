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
import PipelineConfigurations from 'components/PipelineConfigurations';
import {objectQuery} from 'services/helpers';
import classnames from 'classnames';
import Popover from 'components/Popover';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.RunLevel';

export default class RunConfigs extends Component {
  static propTypes = {
    currentRun: PropTypes.object,
    runs: PropTypes.array,
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string
  };

  state = {
    showModeless: false
  };

  getRuntimeArgsAndToggleModeless = () => {
    if (!this.state.showModeless) {
      PipelineConfigurationsStore.dispatch({
        type: PipelineConfigurationsActions.SET_MODELESS_OPEN_STATUS,
        payload: { open: false }
      });

      let runtimeArgs = objectQuery(this.props.currentRun, 'properties', 'runtimeArgs') || '';
      try {
        runtimeArgs = JSON.parse(runtimeArgs);
        delete runtimeArgs[''];
      } catch (e) {
        console.log('ERROR: Cannot parse runtime arguments');
        runtimeArgs = {};
      }

      runtimeArgs = convertMapToKeyValuePairsObj(runtimeArgs);

      PipelineConfigurationsStore.dispatch({
        type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
        payload: { runtimeArgs }
      });

      // Have to set timeout here to make sure any other config modeless will be closed
      // before opening this one
      setTimeout(() => this.toggleModeless());
    } else {
      this.toggleModeless();
    }
  };

  toggleModeless = () => {
    this.setState({
      showModeless: !this.state.showModeless
    });
  };

  renderRunConfigsButton() {
    return (
      <div
        className="run-configs-btn"
        onClick={this.getRuntimeArgsAndToggleModeless}
      >
        <IconSVG name="icon-sliders" />
        <div className="button-label">
          {T.translate(`${PREFIX}.configs`)}
        </div>
      </div>
    );
  }

  render() {
    const ConfigsBtnComp = () => (
      <div className="run-configs-btn">
        <IconSVG name="icon-sliders" />
        <div className="button-label">
          {T.translate(`${PREFIX}.configs`)}
        </div>
      </div>
    );

    if (!this.props.runs.length) {
      return (
        <Popover
          target={ConfigsBtnComp}
          showOn='Hover'
          placement='bottom'
          className="run-info-container run-configs-container disabled"
        >
          {T.translate(`${PREFIX}.pipelineNeverRun`)}
        </Popover>
      );
    }

    return (
      <div className={classnames("run-info-container run-configs-container", {"active" : this.state.showModeless})}>
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
