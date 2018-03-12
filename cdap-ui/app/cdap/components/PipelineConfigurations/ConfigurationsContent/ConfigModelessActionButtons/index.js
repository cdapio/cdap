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
import {applyRuntimeArgs, updatePipeline} from 'components/PipelineConfigurations/Store/ActionCreator';
import ConfigModelessPrimaryActionBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessPrimaryActionBtn';
import ConfigModelessSecondaryActionBtn from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessSecondaryActionBtn';
import ConfigModelessRuntimeArgsCount from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons/ConfigModelessRuntimeArgsCount';

require('./ConfigModelessActionButtons.scss');

export default class ConfigModelessActionButtons extends Component {
  state = {
    // need 2 states here instead of just 1, to determine which button to show
    // spinning wheel on
    updatingPipeline: false,
    updatingPipelineAndRunOrSchedule: false
  };

  static propTypes = {
    onClose: PropTypes.func,
    activeTab: PropTypes.string,
    action: PropTypes.func,
    actionLabel: PropTypes.string,
    isHistoricalRun: PropTypes.bool
  };

  static defaultProps = {
    actionLabel: 'Save and Run'
  };

  closeModeless = () => {
    if (typeof this.props.onClose === 'function') {
      this.props.onClose();
    }
  };

  closeModelessAndRunOrSchedule = () => {
    this.closeModeless();
    if (typeof this.props.action === 'function') {
      this.props.action();
    }
  };

  saveAndRunOrSchedule = (pipelineEdited) => {
    if (!this.props.isHistoricalRun) {
      applyRuntimeArgs();
    }
    if (!pipelineEdited) {
      this.closeModelessAndRunOrSchedule();
      return;
    }

    this.setState({
      updatingPipelineAndRunOrSchedule: true
    });
    updatePipeline()
    .subscribe(() => {
      this.closeModelessAndRunOrSchedule();
    }, (err) => {
      console.log(err);
    }, () => {
      this.setState({
        updatingPipelineAndRunOrSchedule: false
      });
    });
  }

  saveAndCloseModeless = (pipelineEdited) => {
    applyRuntimeArgs();
    if (!pipelineEdited) {
      this.closeModeless();
      return;
    }

    this.setState({
      updatingPipeline: true
    });
    updatePipeline()
    .subscribe(() => {
      this.closeModeless();
    }, (err) => {
      console.log(err);
    }, () => {
      this.setState({
        updatingPipeline: false
      });
    });
  };

  render() {
    return (
      <div className="configuration-step-navigation">
        <div className="apply-action-container">
          <ConfigModelessPrimaryActionBtn
            updatingPipelineAndRunOrSchedule={this.state.updatingPipelineAndRunOrSchedule}
            saveAndRunOrSchedule={this.saveAndRunOrSchedule}
            actionLabel={this.props.actionLabel}
            isHistoricalRun={this.props.isHistoricalRun}
          />
          <ConfigModelessSecondaryActionBtn
            updatingPipeline={this.state.updatingPipeline}
            saveAndCloseModeless={this.saveAndCloseModeless}
            isHistoricalRun={this.props.isHistoricalRun}
          />
          <ConfigModelessRuntimeArgsCount
            activeTab={this.props.activeTab}
            isHistoricalRun={this.props.isHistoricalRun}
          />
        </div>
      </div>
    );
  }
}
