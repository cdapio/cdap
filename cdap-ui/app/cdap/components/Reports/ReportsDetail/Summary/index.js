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

import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { humanReadableDate, humanReadableDuration } from 'services/helpers';
import { GLOBALS } from 'services/global-constants';
import T from 'i18n-react';

const PREFIX = 'features.Reports.ReportsDetail';

require('./Summary.scss');

function renderNamespaces(summary) {
  if (!summary.namespaces) {
    return null;
  }

  return summary.namespaces
    .map((ns) => {
      return ns.namespace + T.translate(`${PREFIX}.numRuns`, { num: ns.runs });
    })
    .join(', ');
}

function renderAppType(summary) {
  if (!summary.artifacts) {
    return null;
  }

  const customAppLabel = T.translate(`${PREFIX}.customApp`);

  let counts = {
    batch: 0,
    realtime: 0,
    [customAppLabel]: 0,
  };
  // BatchPipeline and RealtimePipeline case should be removed once going to real API
  summary.artifacts.forEach((artifact) => {
    switch (artifact.name) {
      case 'BatchPipeline':
      case GLOBALS.etlDataPipeline:
        counts.batch = artifact.runs;
        break;
      case 'RealtimePipeline':
      case GLOBALS.etlDataStreams:
        counts.realtime = artifact.runs;
        break;
      default:
        counts[customAppLabel] += artifact.runs;
    }
  });

  return Object.keys(counts)
    .filter((type) => counts[type] !== 0)
    .map((type) => `${counts[type]} ${type}`)
    .join('; ');
}

function renderDuration(summary) {
  let { durations } = summary;

  if (!durations) {
    return null;
  }

  let min = humanReadableDuration(durations.min);
  let max = humanReadableDuration(durations.max);
  let average = humanReadableDuration(Math.round(durations.average));

  return T.translate(`${PREFIX}.runDuration`, { min, max, average });
}

function renderLastStarted(summary) {
  let { starts } = summary;

  if (!starts) {
    return null;
  }

  return T.translate(`${PREFIX}.lastStarted`, {
    newest: humanReadableDate(starts.newest),
    oldest: humanReadableDate(starts.oldest),
  });
}

function renderOwners(summary) {
  if (!summary.owners) {
    return null;
  }

  return summary.owners.map((owner) => `${owner.user} (${owner.runs})`).join('; ');
}

function renderStartMethod(summary) {
  if (!summary.startMethods) {
    return null;
  }
  const labelMap = {
    MANUAL: T.translate(`${PREFIX}.manually`),
    SCHEDULED: T.translate(`${PREFIX}.bySchedule`),
    PROGRAM_STATUS: T.translate(`${PREFIX}.byTrigger`),
  };

  return summary.startMethods
    .map((startMethod) => `${labelMap[startMethod.method]} (${startMethod.runs})`)
    .join('; ');
}

function SummaryView({ summary }) {
  return (
    <div className="reports-summary-container">
      <div className="row summary-section">
        <div className="col-6">
          <table className="table">
            <tbody>
              <tr className="no-border">
                <td colSpan="2">
                  <strong>{T.translate(`${PREFIX}.reportSummary`)}</strong>
                </td>
              </tr>

              <tr className="no-border">
                <td>{T.translate(`${PREFIX}.namespaceLabel`)}</td>

                <td>{renderNamespaces(summary)}</td>
              </tr>

              <tr>
                <td>{T.translate(`${PREFIX}.timeRangeLabel`)}</td>

                <td>
                  {T.translate(`${PREFIX}.timeRange`, {
                    start: humanReadableDate(summary.start),
                    end: humanReadableDate(summary.end),
                  })}
                </td>
              </tr>

              <tr>
                <td>{T.translate(`${PREFIX}.appTypeLabel`)}</td>

                <td>{renderAppType(summary)}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div className="col-6">
          <table className="table">
            <tbody>
              <tr className="no-border">
                <td>{T.translate(`${PREFIX}.runDurationLabel`)}</td>

                <td>{renderDuration(summary)}</td>
              </tr>

              <tr>
                <td>{T.translate(`${PREFIX}.lastStartedLabel`)}</td>

                <td>{renderLastStarted(summary)}</td>
              </tr>

              <tr>
                <td>{T.translate(`${PREFIX}.ownersLabel`)}</td>

                <td>{renderOwners(summary)}</td>
              </tr>

              <tr>
                <td>{T.translate(`${PREFIX}.startedLabel`)}</td>

                <td>{renderStartMethod(summary)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

SummaryView.propTypes = {
  summary: PropTypes.object,
};

const mapStateToProps = (state) => {
  return {
    summary: state.details.summary,
  };
};

const Summary = connect(mapStateToProps)(SummaryView);

export default Summary;
