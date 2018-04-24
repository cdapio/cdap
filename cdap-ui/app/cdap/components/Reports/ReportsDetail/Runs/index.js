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
import {humanReadableDate, humanReadableDuration} from 'services/helpers';
import {DefaultSelection} from 'components/Reports/store/ActionCreator';
import difference from 'lodash/difference';

require('./Runs.scss');

const PIPELINES = [GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams];

function getName(run) {
  if (!run.applicationName) { return '--'; }

  let name = run.applicationName;

  if (PIPELINES.indexOf(run.artifactName) == -1) {
    name = `${name} - ${run.program}`;
  }

  return name;
}

function getType(run) {
  switch (run.artifactName) {
    case GLOBALS.etlDataPipeline:
      return 'Batch Pipeline';
    case GLOBALS.etlDataStreams:
      return 'Realtime Pipeline';
    default:
      return run.programType;
  }
}

function renderHeader(headers) {
  return (
    <div className="grid-header">
      <div className="grid-row">
        <div>
          Name
        </div>

        <div>
          Type
        </div>

        {
          headers.map((head) => {
            return (
              <div>
                {head}
              </div>
            );
          })
        }
      </div>
    </div>
  );
}

function renderBody(runs, headers) {
  return (
    <div className="grid-body">
      {
        runs.map((run, i) => {
          return (
            <div
              key={i}
              className="grid-row"
            >
              <div>
                {getName(run)}
              </div>

              <div>
                {getType(run)}
              </div>

              {
                headers.map((head) => {
                  let value = run[head];

                  if (['start', 'end'].indexOf(head) !== -1) {
                    value = humanReadableDate(value);
                  }

                  if (head === 'duration') {
                    value = humanReadableDuration(value);
                  }

                  return (
                    <div>
                      {value || '--'}
                    </div>
                  );
                })
              }
            </div>
          );
        })
      }
    </div>
  );
}

function getHeaders(request) {
  if (!request.fields) { return []; }

  let headers = difference(request.fields, DefaultSelection);

  return headers;
}

function RunsView({runs, request}) {
  let headers = getHeaders(request);

  return (
    <div className="reports-runs-container">
      <div className="grid-wrapper">
        <div className="grid grid-container">
          {renderHeader(headers)}
          {renderBody(runs, headers)}
        </div>
      </div>
    </div>
  );
}

RunsView.propTypes = {
  runs: PropTypes.array,
  request: PropTypes.object
};

const mapStateToProps = (state) => {
  return {
    runs: state.details.runs,
    request: state.details.request
  };
};

const Runs = connect(
  mapStateToProps
)(RunsView);

export default Runs;
