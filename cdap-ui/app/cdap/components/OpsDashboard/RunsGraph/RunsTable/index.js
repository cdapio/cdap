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
import moment from 'moment';
import {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';
import {humanReadableDuration} from 'services/helpers';
import T from 'i18n-react';

const PREFIX = 'features.OpsDashboard.RunsGraph.RunsTable';

require('./RunsTable.scss');

const renderHeader = () => {
  return (
    <div className="grid-header">
      <div className="grid-row">
        <div>
          {T.translate(`${PREFIX}.time`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.date`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.totalRunsStarted`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.scheduledTriggered`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.manually`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.totalRunsEnded`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.successful`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.failed`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.running`)}
        </div>
        <div>
          {T.translate(`${PREFIX}.totalStartDelay`)}
        </div>
      </div>
    </div>
  );
};

const renderBody = (data, onRowClick) => {
  return (
    <div className="grid-body">
      {
        data.map((row) => {
          let time = moment(parseInt(row.time, 10));

          return (
            <div
              key={row.time}
              className="grid-row"
              onClick={onRowClick.bind(this, row)}
            >
              <div className="column-time">
                {time.format('ha')}
              </div>
              <div className="column-date">
                {time.format('ddd. MMM D, YYYY')}
              </div>
              <div className="column-total-start">
                {row.schedule + row.manual}
              </div>
              <div className="column-schedule">
                {row.schedule}
              </div>
              <div className="column-manual">
                {row.manual}
              </div>
              <div className="column-total-runs">
                {row.successful + row.failed}
              </div>
              <div className="column-successful">
                {row.successful}
              </div>
              <div className="column-failed">
                {row.failed}
              </div>
              <div className="column-running">
                {row.running}
              </div>
              <div className="column-delay">
                {humanReadableDuration(row.delay)}
              </div>
            </div>
          );
        })
      }
    </div>
  );
};

function RunsTableView({data, onRowClick}) {
  return (
    <div className="runs-table-container grid-wrapper">
      <div className="grid grid-container">
        {renderHeader()}

        {renderBody(data, onRowClick)}
      </div>
    </div>
  );
}

RunsTableView.propTypes = {
  data: PropTypes.array,
  onRowClick: PropTypes.func
};

const mapStateToProps = (state) => {
  return {
    data: state.dashboard.data
  };
};

const mapDispatch = (dispatch) => {
  return {
    onRowClick: (data) => {
      dispatch({
        type: DashboardActions.setDisplayBucket,
        payload: {
          displayBucketInfo: data
        }
      });
    }
  };
};

const RunsTable = connect(
  mapStateToProps,
  mapDispatch
)(RunsTableView);

export default RunsTable;
