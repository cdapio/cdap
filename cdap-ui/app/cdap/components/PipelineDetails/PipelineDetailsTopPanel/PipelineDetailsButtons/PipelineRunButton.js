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
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import { runPipeline } from 'components/PipelineConfigurations/Store/ActionCreator';
import { setRunError } from 'components/PipelineDetails/store/ActionCreator';
import PipelineRuntimeArgsDropdownBtn from 'components/PipelineDetails/PipelineRuntimeArgsDropdownBtn';
import PipelineConfigurationsStore from 'components/PipelineConfigurations/Store';
import { convertKeyValuePairsToMap } from 'services/helpers';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineRunButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    runButtonLoading: PropTypes.bool,
    runError: PropTypes.string,
    runtimeArgs: PropTypes.array,
  };

  state = {
    showRunOptions: false,
  };

  toggleRunConfigOption = (showRunOptions) => {
    if (showRunOptions === this.state.showRunOptions) {
      return;
    }
    this.setState({
      showRunOptions: showRunOptions || !this.state.showRunOptions,
    });
  };

  runPipelineOrToggleConfig = () => {
    if (this.props.runButtonLoading) {
      return;
    }

    if (!this.state.showRunOptions) {
      let { isMissingKeyValues } = PipelineConfigurationsStore.getState();
      if (isMissingKeyValues) {
        this.toggleRunConfigOption();
      } else {
        let { runtimeArgs } = this.props;
        // Arguments with empty values are assumed to be provided from the pipeline
        runtimeArgs.pairs = runtimeArgs.pairs.filter((runtimeArg) => !runtimeArg.value);
        let runtimeArgsMap = convertKeyValuePairsToMap(runtimeArgs.pairs);
        runPipeline(runtimeArgsMap);
      }
    }
  };

  renderRunError() {
    if (!this.props.runError) {
      return null;
    }

    return (
      <Alert
        message={this.props.runError}
        type="error"
        showAlert={true}
        onClose={setRunError.bind(null, '')}
      />
    );
  }

  renderRunDropdownBtn = () => {
    return (
      <PipelineRuntimeArgsDropdownBtn
        showRunOptions={this.state.showRunOptions}
        onToggle={this.toggleRunConfigOption}
        disabled={this.props.runButtonLoading}
      />
    );
  };

  renderPipelineRunButton() {
    return (
      <div
        data-cy="pipeline-run-btn"
        onClick={this.runPipelineOrToggleConfig}
        className="btn btn-secondary pipeline-action-btn pipeline-run-btn"
        disabled={this.props.runButtonLoading}
      >
        <div className="btn-container">
          {this.props.runButtonLoading ? (
            <span className="text-success">
              <IconSVG name="icon-spinner" className="fa-spin" />
              <div className="button-label">{T.translate(`${PREFIX}.starting`)}</div>
            </span>
          ) : (
            <span className="text-success">
              <IconSVG name="icon-play" />
              <div className="button-label">{T.translate(`${PREFIX}.run`)}</div>
            </span>
          )}
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-action-container pipeline-run-container">
        {this.renderRunError()}
        {this.renderPipelineRunButton()}
        {this.renderRunDropdownBtn()}
      </div>
    );
  }
}
