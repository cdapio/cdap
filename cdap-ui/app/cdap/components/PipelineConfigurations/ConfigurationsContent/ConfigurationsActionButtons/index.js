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
import {applyRuntimeArgs, updatePipeline, runPipeline, schedulePipeline} from 'components/PipelineConfigurations/Store/ActionCreator';
import ConfigModelessSaveAndRunBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigurationsActionButtons/ConfigModelessSaveAndRunBtn';
import ConfigModelessSaveAndScheduleBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigurationsActionButtons/ConfigModelessSaveAndScheduleBtn';
import ConfigModelessSaveBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigurationsActionButtons/ConfigModelessSaveBtn';
import ConfigModelessRuntimeArgsCount from 'components/PipelineConfigurations/ConfigurationsContent/ConfigurationsActionButtons/ConfigModelessRuntimeArgsCount';

require('./ConfigurationsActionButtons.scss');

export default class ConfigurationsActionButtons extends Component {
  state = {
    saveLoading: false,
    saveAndRunLoading: false,
    saveAndScheduleLoading: false
  };

  static propTypes = {
    onClose: PropTypes.func,
    activeTab: PropTypes.string,
    action: PropTypes.string
  };

  static defaultProps = {
    action: 'run'
  };

  closeModeless = () => {
    if (typeof this.props.onClose === 'function') {
      this.props.onClose();
    }
  };

  closeModelessAndRun = () => {
    this.closeModeless();
    runPipeline();
  };

  closeModelessAndSchedule = () => {
    this.closeModeless();
    schedulePipeline();
  };

  saveAndRun = (pipelineEdited) => {
    this.saveAndAction(pipelineEdited, 'saveAndRunLoading', this.closeModelessAndRun);
  }

  saveAndSchedule = (pipelineEdited) => {
    this.saveAndAction(pipelineEdited, 'saveAndScheduleLoading', this.closeModelessAndSchedule);
  }

  saveConfig = (pipelineEdited) => {
    this.saveAndAction(pipelineEdited, 'saveLoading', this.closeModeless);
  };

  saveAndAction(pipelineEdited, loadingState, actionFn) {
    applyRuntimeArgs();
    if (!pipelineEdited) {
      actionFn();
      return;
    }

    this.setState({
      [loadingState]: true
    });
    updatePipeline()
    .subscribe(() => {
      actionFn();
    }, (err) => {
      console.log(err);
    }, () => {
      this.setState({
        [loadingState]: false
      });
    });
  }

  render() {
    let SaveAndActionComp;
    if (this.props.action === 'run') {
      SaveAndActionComp = (
        <ConfigModelessSaveAndRunBtn
          saveAndRun={this.saveAndRun}
          saveAndRunLoading={this.state.saveAndRunLoading}
        />
      );
    } else if (this.props.action === 'schedule') {
      SaveAndActionComp = (
        <ConfigModelessSaveAndScheduleBtn
          saveAndSchedule={this.saveAndSchedule}
          saveAndScheduleLoading={this.state.saveAndScheduleLoading}
        />
      );
    }
    return (
      <div className="configuration-step-navigation">
        <div className="apply-run-container">
          {SaveAndActionComp}
          <ConfigModelessSaveBtn
            saveConfig={this.saveConfig}
            saveLoading={this.state.saveLoading}
          />
          <ConfigModelessRuntimeArgsCount
            activeTab={this.props.activeTab}
          />
        </div>
      </div>
    );
  }
}
