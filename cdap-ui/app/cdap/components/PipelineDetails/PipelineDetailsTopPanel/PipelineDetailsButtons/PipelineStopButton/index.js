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
import {MyProgramApi} from 'api/program';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {GLOBALS} from 'services/global-constants';
import PipelineStopPopover from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineStopButton/PipelineStopPopover';

export default class PipelineStopButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    currentRun: PropTypes.object,
    runs: PropTypes.array
  }

  state = {
    loading: false,
    stopError: null,
    disabled: false,
    runningRuns: []
  };

  componentWillReceiveProps(nextProps) {
    let runningRuns = nextProps.runs.filter(run => run.status === 'RUNNING');
    this.setState({
      runningRuns,
      disabled: runningRuns.length === 0
    });
  }

  stopPipeline = () => {
    if (this.state.loading || this.state.disabled) {
      return;
    }

    this.setState({ loading: true });
    this.stopRun()
    .subscribe(() => {
      this.setState({
        loading: false
      });
    }, (err) => {
      this.setState({
        loading: false,
        stopError: err.response || err
      });
    });
  }

  stopRun = (runId = this.props.currentRun.runid) => {
    let pipelineType = this.props.isBatch ? GLOBALS.etlDataPipeline : GLOBALS.etlDataStreams;
    let params = {
      namespace: getCurrentNamespace(),
      appId: this.props.pipelineName,
      programType: GLOBALS.programType[pipelineType],
      programId: GLOBALS.programId[pipelineType],
      runId
    };
    return MyProgramApi.stopRun(params);
    // .subscribe(() => {
    //   this.setState({
    //     loading: false
    //   });
    // }, (err) => {
    //   this.setState({
    //     loading: false,
    //     stopError: err.response || err
    //   });
    // });
  }

  renderStopError() {
    if (!this.state.stopError) {
      return null;
    }

    return (
      <Alert
        message={this.state.stopError}
        type='error'
        showAlert={true}
        onClose={() => this.setState({
          stopError: null
        })}
      />
    );
  }

  renderPipelineStopButton() {
    if (this.state.runningRuns.length > 1) {
      return (
        <PipelineStopPopover
          runs={this.state.runningRuns}
          stopRun={this.stopRun}
        />
      );
    }
    return (
      <div
        onClick={this.stopPipeline}
        className="btn pipeline-action-btn pipeline-stop-btn"
        disabled={this.state.loading || this.state.disabled}
      >
        <div className="btn-container">
          {
            this.state.loading ?
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            :
              (
                <span>
                  <IconSVG name="icon-stop" />
                  <div className="button-label">
                    Stop
                  </div>
                </span>
              )
          }
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-action-container pipeline-stop-container">
        {this.renderStopError()}
        {this.renderPipelineStopButton()}

      </div>
    );
  }
}
