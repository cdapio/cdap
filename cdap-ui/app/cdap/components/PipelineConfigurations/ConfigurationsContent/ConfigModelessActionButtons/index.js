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
import {
  applyRuntimeArgs,
  updatePipeline,
  runPipeline,
  schedulePipeline,
  updatePreferences
} from 'components/PipelineConfigurations/Store/ActionCreator';
import ConfigModelessSaveAndRunBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessSaveAndRunBtn';
import ConfigModelessSaveAndScheduleBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessSaveAndScheduleBtn';
import ConfigModelessSaveBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessSaveBtn';
import ConfigModelessRuntimeArgsCount from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessRuntimeArgsCount';
import ConfigModelessCopyRuntimeArgsBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessCopyRuntimeArgsBtn';
import {Observable} from 'rxjs/Observable';

require('./ConfigModelessActionButtons.scss');

export default class ConfigModelessActionButtons extends Component {
  state = {
    saveLoading: false,
    saveAndRunLoading: false,
    saveAndScheduleLoading: false,
    runtimeArgsCopied: false
  };

  static propTypes = {
    onClose: PropTypes.func,
    activeTab: PropTypes.string,
    action: PropTypes.string,
    isHistoricalRun: PropTypes.bool
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
  };

  saveAndSchedule = (pipelineEdited) => {
    this.saveAndAction(pipelineEdited, 'saveAndScheduleLoading', this.closeModelessAndSchedule);
  };

  saveConfig = (pipelineEdited) => {
    this.saveAndAction(pipelineEdited, 'saveLoading', this.closeModeless);
  };

  saveAndAction = (pipelineEdited, loadingState, actionFn) => {
    if (!pipelineEdited) {
      actionFn();
      return;
    }
    applyRuntimeArgs();

    this.setState({
      [loadingState]: true
    });
    Observable.forkJoin(
      updatePipeline(),
      updatePreferences()
    )
      .subscribe(() => {
        actionFn();
      }, (err) => {
        console.log(err);
      }, () => {
        this.setState({
          [loadingState]: false
        });
      });
  };

  setRuntimeArgsCopiedState = () => {
    this.setState({
      runtimeArgsCopied: true
    });
  };

  render() {
    let ActionComp;
    if (this.props.action === 'run') {
      ActionComp = (
        <ConfigModelessSaveAndRunBtn
          saveAndRun={this.saveAndRun}
          saveAndRunLoading={this.state.saveAndRunLoading}
        />
      );
    } else if (this.props.action === 'schedule') {
      ActionComp = (
        <ConfigModelessSaveAndScheduleBtn
          saveAndSchedule={this.saveAndSchedule}
          saveAndScheduleLoading={this.state.saveAndScheduleLoading}
        />
      );
    } else if (this.props.action === 'copy') {
      ActionComp = (
        <ConfigModelessCopyRuntimeArgsBtn
          setRuntimeArgsCopiedState={this.setRuntimeArgsCopiedState}
          runtimeArgsCopied={this.state.runtimeArgsCopied}
        />
      );
    }
    return (
      <div className="configuration-step-navigation">
        <div className="apply-action-container">
          {ActionComp}
          {
            !this.props.isHistoricalRun ?
              <ConfigModelessSaveBtn
                saveConfig={this.saveConfig}
                saveLoading={this.state.saveLoading}
              />
            :
              null
          }
          <ConfigModelessRuntimeArgsCount
            activeTab={this.props.activeTab}
            isHistoricalRun={this.props.isHistoricalRun}
          />
        </div>
      </div>
    );
  }
}
