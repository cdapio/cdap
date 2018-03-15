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
import {runPipeline} from 'components/PipelineConfigurations/Store/ActionCreator';
import {setRunError} from 'components/PipelineDetails/store/ActionCreator';
import {keyValuePairsHaveMissingValues} from 'components/KeyValuePairs/KeyValueStoreActions';
import PipelineConfigurations from 'components/PipelineConfigurations';

export default class PipelineRunButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    runButtonLoading: PropTypes.bool,
    runError: PropTypes.string,
    runtimeArgs: PropTypes.array
  }

  state = {
    showConfigModeless: false
  };

  toggleConfigModeless = () => {
    this.setState({
      showConfigModeless: !this.state.showConfigModeless
    });
  };

  runPipelineOrToggleConfig = () => {
    if (this.props.runButtonLoading) {
      return;
    }

    if (keyValuePairsHaveMissingValues(this.props.runtimeArgs)) {
      this.toggleConfigModeless();
    } else {
      runPipeline();
    }
  };

  renderRunError() {
    if (!this.props.runError) {
      return null;
    }

    return (
      <Alert
        message={this.props.runError}
        type='error'
        showAlert={true}
        onClose={setRunError.bind(null, '')}
      />
    );
  }

  renderConfigModeless() {
    if (!this.state.showConfigModeless) { return null; }

    return (
      <PipelineConfigurations
        onClose={this.toggleConfigModeless}
        isDetailView={true}
        isBatch={this.props.isBatch}
        pipelineName={this.props.pipelineName}
      />
    );
  }

  renderPipelineRunButton() {
    return (
      <div
        onClick={this.runPipelineOrToggleConfig}
        className="btn pipeline-action-btn pipeline-run-btn"
        disabled={this.props.runButtonLoading}
      >
        <div className="btn-container">
          {
            this.props.runButtonLoading ?
              (
                <span className="text-success">
                  <IconSVG name="icon-spinner" className="fa-spin" />
                  <div className="button-label">Starting</div>
                </span>
              )

            :
              (
                <span className="text-success">
                  <IconSVG name="icon-play"/>
                  <div className="button-label">Run</div>
                </span>
              )
          }
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-action-container pipeline-run-container">
        {this.renderRunError()}
        {this.renderPipelineRunButton()}
        {this.renderConfigModeless()}
      </div>
    );
  }
}
