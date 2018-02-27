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
import React from 'react';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import Duration from 'components/Duration';
import moment from 'moment';
require('./PipelineStopPopover.scss');

export default function PipelineStopPopver({runs, stopRun}) {
  const stopBtnAndLabel = () => {
    return (
      <div className="btn pipeline-action-btn pipeline-stop-btn">
        <div className="btn-container">
          <IconSVG name="icon-stop" />
          <div className="button-label">
            Stop
          </div>
        </div>
      </div>
    );
  };

  const stopAllRuns = () => {
    runs.forEach(run => {
      stopRun(run.runid);
    });
  };

  return (
    <Popover
      target={stopBtnAndLabel}
      className="stop-btn-popover"
      placement="bottom"
      bubbleEvent={false}
      enableInteractionInPopover={true}
    >
      <div className="stop-btn-popover-header">
        <strong>{`Current runs (${runs.length}`})</strong>
        <span
          className="stop-all-btn"
          onClick={stopAllRuns}
        >
          <IconSVG name="icon-stop" />
          <span>Stop All</span>
        </span>
      </div>
      <table className="stop-btn-popover-table table">
        <thead>
          <tr>
            <th>Start Time</th>
            <th>Duration</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            runs.map((run, i) => {
              return (
                <tr key={i}>
                  <td>{moment.unix(run.start).calendar()}</td>
                  <td>
                    <Duration
                      targetTime={run.start}
                      isMillisecond={false}
                      showFullDuration={true}
                    />
                  </td>
                  <td>
                    <a onClick={stopRun.bind(null, run.runid)}>
                      Stop run
                    </a>
                  </td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </Popover>
  );
}

PipelineStopPopver.propTypes = {
  runs: PropTypes.array,
  stopRun: PropTypes.function
};
