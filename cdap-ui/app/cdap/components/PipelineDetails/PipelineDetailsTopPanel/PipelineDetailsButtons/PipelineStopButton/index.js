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
import { MyProgramApi } from 'api/program';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { GLOBALS } from 'services/global-constants';
import PipelineStopPopover from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineStopButton/PipelineStopPopover';
import { setStopButtonLoading, setStopError } from 'components/PipelineDetails/store/ActionCreator';
import isEqual from 'lodash/isEqual';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineStopButton extends Component {
  static propTypes = {
    pipelineType: PropTypes.string,
    pipelineName: PropTypes.string,
    currentRun: PropTypes.object,
    runs: PropTypes.array,
    stopButtonLoading: PropTypes.bool,
    stopError: PropTypes.string,
  };

  state = {
    disabled: true,
    activeRuns: [],
  };

  activeStatuses = ['PENDING', 'STARTING', 'RUNNING'];

  componentWillReceiveProps(nextProps) {
    let activeRuns = nextProps.runs.filter((run) => {
      return this.activeStatuses.indexOf(run.status) !== -1;
    });
    if (!isEqual(this.state.activeRuns, activeRuns)) {
      this.setState({
        activeRuns,
        disabled: activeRuns.length === 0,
      });
    }
  }

  stopPipeline = () => {
    if (this.props.stopButtonLoading || this.state.disabled) {
      return;
    }

    setStopButtonLoading(true);
    this.stopRun().subscribe(
      () => {},
      (err) => {
        setStopButtonLoading(false);
        setStopError(err.response || err);
      }
    );
  };

  stopRun = (runId = this.props.runs[0].runid) => {
    let params = {
      namespace: getCurrentNamespace(),
      appId: this.props.pipelineName,
      programType: GLOBALS.programType[this.props.pipelineType],
      programId: GLOBALS.programId[this.props.pipelineType],
      runId,
    };
    return MyProgramApi.stopRun(params);
  };

  renderStopError() {
    if (!this.props.stopError) {
      return null;
    }

    return (
      <Alert
        message={this.props.stopError}
        type="error"
        showAlert={true}
        onClose={() => {
          setStopError(null);
        }}
      />
    );
  }

  renderPipelineStopButton() {
    if (this.state.activeRuns.length > 1) {
      return (
        <PipelineStopPopover
          runs={this.state.activeRuns}
          currentRunId={this.props.currentRun.runid}
          stopRun={this.stopRun}
          pipelineId={this.props.pipelineName}
        />
      );
    }
    return (
      <div
        onClick={this.stopPipeline}
        className="btn pipeline-action-btn pipeline-stop-btn"
        disabled={this.props.stopButtonLoading || this.state.disabled}
      >
        <div className="btn-container">
          {this.props.stopButtonLoading ? (
            <span>
              <IconSVG name="icon-spinner" className="fa-spin" />
              <div className="button-label">{T.translate(`${PREFIX}.stopping`)}</div>
            </span>
          ) : (
            <span>
              <IconSVG name="icon-stop" />
              <div className="button-label">{T.translate(`${PREFIX}.stop`)}</div>
            </span>
          )}
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
