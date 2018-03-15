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
import classnames from 'classnames';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {reverseArrayWithoutMutating} from 'services/helpers';
import findIndex from 'lodash/findIndex';
import T from 'i18n-react';
require('./RunningRunsPopover.scss');

const PREFIX = 'features.PipelineDetails';

export default function RunningRunsPopover({runs, currentRunId, pipelineId}) {
  let reversedRuns = reverseArrayWithoutMutating(runs);
  let currentRunIndex = findIndex(reversedRuns, {runid: currentRunId});

  const runningRunsLabel = () => {
    return (
      <span className="running-runs-toggle">
        <a>
          {T.translate(`${PREFIX}.RunLevel.currentIndex`, {
            currentRunIndex: currentRunIndex + 1,
            numRuns: runs.length
          })}
        </a>
      </span>
    );
  };

  const navigateToRun = (runId) => {
    let runIdUrl = window.getHydratorUrl({
      stateName: 'hydrator.detail',
      stateParams: {
        namespace: getCurrentNamespace(),
        pipelineId
      }
    });
    runIdUrl += `?runid=${runId}`;
    window.location.href = runIdUrl;
  };

  return (
    <Popover
      target={runningRunsLabel}
      className="running-runs-popover"
      placement="bottom"
      bubbleEvent={false}
      enableInteractionInPopover={true}
    >
      <div>
        <strong>{T.translate(`${PREFIX}.RunLevel.runsCurrentlyRunning`)}</strong>
      </div>
      <table className="running-runs-popover-table table">
        <thead>
          <tr>
            <th></th>
            <th>{T.translate(`${PREFIX}.startTime`)}</th>
            <th>{T.translate(`${PREFIX}.duration`)}</th>
          </tr>
        </thead>
        <tbody>
          {
            reversedRuns.map((run, i) => {
              return (
                <tr
                  key={i}
                  className={classnames({"current-run-row": run.runid === currentRunId})}
                  onClick={navigateToRun.bind(null, run.runid)}
                >
                  <td>
                    {
                      run.runid === currentRunId ?
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
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </Popover>
  );
}

RunningRunsPopover.propTypes = {
  runs: PropTypes.array,
  currentRunId: PropTypes.string,
  pipelineId: PropTypes.string
};
