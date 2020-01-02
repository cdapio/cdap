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
import {
  updatePipeline,
  runPipeline,
  schedulePipeline,
  updatePreferences,
} from 'components/PipelineConfigurations/Store/ActionCreator';
import { setRunError } from 'components/PipelineDetails/store/ActionCreator';
import ConfigModelessSaveBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessSaveBtn';
import { Observable } from 'rxjs/Observable';

require('./ConfigModelessActionButtons.scss');

export default class ConfigModelessActionButtons extends Component {
  state = {
    saveLoading: false,
    saveAndRunLoading: false,
    saveAndScheduleLoading: false,
    runtimeArgsCopied: false,
  };

  static propTypes = {
    onClose: PropTypes.func,
    isHistoricalRun: PropTypes.bool,
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

  saveConfig = () => {
    this.saveAndAction('saveLoading', this.closeModeless);
  };

  saveAndAction = (loadingState, actionFn) => {
    this.setState({
      [loadingState]: true,
    });
    Observable.forkJoin(updatePipeline(), updatePreferences()).subscribe(
      () => {
        actionFn();
        this.setState({
          [loadingState]: false,
        });
      },
      (err) => {
        setRunError(err.response || err);
        this.setState({
          [loadingState]: false,
        });
      }
    );
  };

  setRuntimeArgsCopiedState = () => {
    this.setState({
      runtimeArgsCopied: true,
    });
  };

  render() {
    return (
      <div className="configuration-step-navigation">
        <div className="apply-action-container">
          {!this.props.isHistoricalRun ? (
            <ConfigModelessSaveBtn
              saveConfig={this.saveConfig}
              saveLoading={this.state.saveLoading}
            />
          ) : null}
        </div>
      </div>
    );
  }
}
