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
import {connect} from 'react-redux';
import {GLOBALS} from 'services/global-constants';
import {humanReadableDate} from 'services/helpers';

require('./Runs.scss');

const PIPELINES = [GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams];

function getName(run) {
  let name = run.application.name;

  if (PIPELINES.indexOf(run.artifact.name) == -1) {
    name = `${name} - ${run.program}`;
  }

  return name;
}

function getType(run) {
  switch (run.artifact.name) {
    case GLOBALS.etlDataPipeline:
      return 'Batch Pipeline';
    case GLOBALS.etlDataStreams:
      return 'Realtime Pipeline';
    default:
      return run.type;
  }
}

function renderHeader() {
  return (
    <div className="grid-header">
      <div className="grid-row">
        <div>Namespace</div>
        <div>Name</div>
        <div>Type</div>
        <div>Duration</div>
        <div>Start time</div>
        <div>End time</div>
        <div>User</div>
        <div>Start method</div>
        <div># Log errors</div>
        <div># Log warnings</div>
        <div># Records out</div>
      </div>
    </div>
  );
}

function renderBody(runs) {
  return (
    <div className="grid-body">
      {
        runs.map((run, i) => {
          return (
            <div
              key={i}
              className="grid-row"
            >
              <div>{run.namespace}</div>
              <div>{getName(run)}</div>
              <div>{getType(run)}</div>
              <div>{run.duration}</div>
              <div>{humanReadableDate(run.start)}</div>
              <div>{humanReadableDate(run.end)}</div>
              <div>{run.user}</div>
              <div>{run.startMethod}</div>
              <div>{run.numLogErrors}</div>
              <div>{run.numLogWarnings}</div>
              <div>{run.numRecordsOut}</div>
            </div>
          );
        })
      }
    </div>
  );
}

function RunsView({runs}) {
  return (
    <div className="reports-runs-container">
      <div className="grid-wrapper">
        <div className="grid grid-container">
          {renderHeader()}
          {renderBody(runs)}
        </div>
      </div>
    </div>
  );
}

RunsView.propTypes = {
  runs: PropTypes.array
};

const mapStateToProps = (state) => {
  return {
    runs: state.details.runs
  };
};

const Runs = connect(
  mapStateToProps
)(RunsView);

export default Runs;
