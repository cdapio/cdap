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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import Duration from 'components/Duration';
import moment from 'moment';
import {Observable} from 'rxjs/Observable';
import {setStopError} from 'components/PipelineDetails/store/ActionCreator';
import classnames from 'classnames';
import T from 'i18n-react';
require('./PipelineStopPopover.scss');

const PREFIX = 'features.PipelineDetails.TopPanel.StopPopover';

export default class PipelineStopPopover extends Component {
  static propTypes = {
    runs: PropTypes.array,
    currentRunId: PropTypes.string,
    stopRun: PropTypes.func
  };

  state = {
    runsBeingStopped: [],
    stopAllBtnLoading: false
  };

  componentWillReceiveProps(nextProps) {
    let runs = nextProps.runs;

    // Here 'nextProps.runs' refer to running runs, so the runs that were being stopped
    // won't even show up in the popover anymore, but we should still reset their states
    if (!runs || !runs.length) {
      this.setState({
        runsBeingStopped: [],
        stopAllBtnLoading: false
      });
    } else {
      let runsBeingStopped = [...this.state.runsBeingStopped];
      this.state.runsBeingStopped.forEach(runid => {
        if (runs.indexOf(runid) === -1) {
          runsBeingStopped.splice(runsBeingStopped.indexOf(runid), 1);
        }
      });
      this.setState({ runsBeingStopped });
    }
  }

  stopBtnAndLabel = () => {
    return (
      <div className="btn pipeline-action-btn pipeline-stop-btn">
        <div className="btn-container">
          <IconSVG name="icon-stop" />
          <div className="button-label">
            {T.translate('features.PipelineDetails.TopPanel.stop')}
          </div>
        </div>
      </div>
    );
  };

  stopAllRuns = () => {
    if (!Array.isArray(this.props.runs) || typeof this.props.stopRun !== 'function') {
      return;
    }

    this.setState({ stopAllBtnLoading: true });
    let stopRunObservables = this.props.runs.map(run => {
      return this.props.stopRun(run.runid);
    });
    Observable.forkJoin(stopRunObservables)
    .subscribe(() => {},
      (err) => {
        setStopError(err.response || err);
        this.setState({ stopAllBtnLoading: false });
      });
  };

  stopSingleRun = (runid) => {
    if (this.state.stopAllBtnLoading || this.state.runsBeingStopped.indexOf(runid) !== -1 || typeof this.props.stopRun !== 'function') {
      return;
    }

    this.setState({
      runsBeingStopped: [...this.state.runsBeingStopped, runid]
    });
    this.props.stopRun(runid)
    .subscribe(() => {},
      (err) => {
        setStopError(err.response || err);
        this.setState({
          runsBeingStopped: this.state.runsBeingStopped.filter(loadingRunId => loadingRunId !== runid)
        });
      });
  }

  render() {
    return (
      <Popover
        target={this.stopBtnAndLabel}
        className="stop-btn-popover"
        placement="bottom"
        bubbleEvent={false}
        enableInteractionInPopover={true}
      >
        <fieldset disabled={this.state.stopAllBtnLoading}>
          <div className="stop-btn-popover-header">
            <strong>
              {T.translate(`${PREFIX}.currentRuns`, {numRuns: this.props.runs.length})}
            </strong>
            <button
              className="stop-all-btn"
              onClick={this.stopAllRuns}
            >
              <IconSVG name="icon-stop" />
              <span>{T.translate(`${PREFIX}.stopAll`)}</span>
              {
                this.state.stopAllBtnLoading ?
                  <IconSVG
                    name="icon-spinner"
                    className="fa-spin"
                  />
                :
                  null
              }
            </button>
          </div>
          <table className="stop-btn-popover-table table">
            <thead>
              <tr>
                <th></th>
                <th>{T.translate('features.PipelineDetails.startTime')}</th>
                <th>{T.translate('features.PipelineDetails.duration')}</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {
                this.props.runs.map((run, i) => {
                  return (
                    <tr
                      key={i}
                      className={classnames({"current-run-row": run.runid === this.props.currentRunId})}
                    >
                      <td>
                        {
                          run.runid === this.props.currentRunId ?
                            <IconSVG name="icon-check" />
                          :
                            null
                        }
                      </td>
                      <td>{moment.unix(run.start).calendar()}</td>
                      <td>
                        <Duration
                          targetTime={run.start}
                          isMillisecond={false}
                          showFullDuration={true}
                        />
                      </td>
                      <td>
                        {
                          this.state.runsBeingStopped.indexOf(run.runid) !== -1 ?
                            <IconSVG
                              name="icon-spinner"
                              className="fa-spin"
                            />
                          :
                            (
                              <a onClick={this.stopSingleRun.bind(this, run.runid)}>
                                {T.translate(`${PREFIX}.stopRun`)}
                              </a>
                            )
                        }
                      </td>
                    </tr>
                  );
                })
              }
            </tbody>
          </table>
        </fieldset>
      </Popover>
    );
  }
}
