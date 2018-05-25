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
import moment from 'moment';
import {connect} from 'react-redux';
import {humanReadableDuration} from 'services/helpers';

require('./RunsList.scss');

function renderHeader() {
  return (
    <div className="grid-header">
      <div className="grid-row">
        <div>Namespace</div>
        <div>Name</div>
        <div>Type</div>
        <div>Duration</div>
        <div>User</div>
        <div>Start Method</div>
        <div>Status</div>
      </div>
    </div>
  );
}

function renderBody(data) {
  return (
    <div className="grid-body">
      {
        data.map((run, i) => {
          let duration = run.end ? run.end - run.start : '--';
          duration = humanReadableDuration(duration);
          return (
            <div
              className="grid-row"
              key={`${run.application.name}${run.program}${run.start}${i}`}
            >
              <div>{run.namespace}</div>
              <div>{run.application.name}</div>
              <div>{run.type}</div>
              <div>{duration}</div>
              <div>{run.user || '--'}</div>
              <div>{run.startMethod}</div>
              <div>{run.status}</div>
            </div>
          );
        })
      }
    </div>
  );
}

function renderGrid(data) {
  if (data.length === 0) {
    return (
      <div className="list-view">
        <h3 className="text-xs-center">No runs</h3>
      </div>
    );
  }

  return (
    <div className="list-view grid-wrapper">
      <div className="grid grid-container">
        {renderHeader()}
        {renderBody(data)}
      </div>
    </div>
  );
}


function RunsListView({bucketInfo, displayRunsList}) {
  if (!displayRunsList) { return null; }

  if (!bucketInfo) {
    return (
      <div className="runs-list-container">
        <h3 className="text-xs-center">No data</h3>
      </div>
    );
  }

  let date = parseInt(bucketInfo.time, 10);

  let timeRangeStart = moment(date).format('h a');
  let timeRangeStop = moment(date).add(1, 'h').format('h a');

  return (
    <div className="runs-list-container">
      <div className="top-panel-data">
        <div className="date">
          <div className="title">Date</div>
          <div>{moment(date).format('ddd, MMM D, YYYY')}</div>
        </div>

        <div className="time-range">
          <div className="title">Time range</div>
          <div>{timeRangeStart} - {timeRangeStop}</div>
        </div>

        <div className="type">
          <div className="title">Type</div>
          <div>All</div>
        </div>

        <div className="start-method">
          <div className="title">Start method</div>
          <div>View All</div>
        </div>

        <div className="status">
          <div className="title">Status</div>
          <div>All</div>
        </div>
      </div>

      {renderGrid(bucketInfo.runsList)}

    </div>
  );
}

RunsListView.propTypes = {
  bucketInfo: PropTypes.object,
  displayRunsList: PropTypes.bool
};

const mapStateToProps = (state) => {
  return {
    bucketInfo: state.dashboard.displayBucketInfo,
    displayRunsList: state.dashboard.displayRunsList
  };
};

const RunsList = connect(
  mapStateToProps
)(RunsListView);

export default RunsList;
