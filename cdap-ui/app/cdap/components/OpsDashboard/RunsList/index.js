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
import { humanReadableDuration, humanReadableDate } from 'services/helpers';
import SortableStickyGrid from 'components/SortableStickyGrid';
import capitalize from 'lodash/capitalize';
import StatusMapper from 'services/StatusMapper';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import TopPanel from 'components/OpsDashboard/RunsList/TopPanel';

const PREFIX = 'features.OpsDashboard.RunsList';

const GRID_HEADERS = [
  {
    property: 'namespace',
    label: T.translate(`${PREFIX}.namespace`),
  },
  {
    property: (run) => run.application.name,
    label: T.translate(`${PREFIX}.name`),
  },
  {
    property: 'type',
    label: T.translate(`${PREFIX}.type`),
  },
  {
    property: 'starting',
    label: T.translate(`${PREFIX}.start`),
  },
  {
    property: (run) => (run.end ? run.end - run.starting : 0),
    label: T.translate(`${PREFIX}.duration`),
  },
  {
    property: 'user',
    label: T.translate(`${PREFIX}.user`),
  },
  {
    property: 'startMethod',
    label: T.translate(`${PREFIX}.startMethod`),
  },
  {
    property: (run) => StatusMapper.lookupDisplayStatus(run.status),
    label: T.translate(`${PREFIX}.status`),
  },
];

require('./RunsList.scss');

function renderBody(data) {
  return (
    <div className="grid-body">
      {data.map((run, i) => {
        let duration = run.end ? run.end - run.running : '--';
        duration = humanReadableDuration(duration);

        const displayStatus = StatusMapper.lookupDisplayStatus(run.status) || '';

        let startTime = run.running || run.starting;
        startTime = humanReadableDate(startTime, false);

        const user = run.user || '--';

        const statusIcon = (
          <IconSVG name="icon-circle" className={`${displayStatus.toLowerCase()}`} />
        );

        return (
          <div
            className="grid-row"
            key={`${run.application.name}${run.program}${run.starting}${i}`}
          >
            <div title={run.namespace}>{run.namespace}</div>
            <div title={run.application.name}>{run.application.name}</div>
            <div>{T.translate(`commons.entity.${run.type.toLowerCase()}.singular`)}</div>
            <div>{startTime}</div>
            <div>{duration}</div>
            <div title={user}>{user}</div>
            <div>{capitalize(run.startMethod)}</div>
            <div>
              {displayStatus ? statusIcon : '--'}
              <span>{displayStatus}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function renderGrid(data) {
  if (data.length === 0) {
    return (
      <div className="list-view">
        <h5 className="text-center no-runs">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{T.translate(`${PREFIX}.noRuns`)}</span>
        </h5>
      </div>
    );
  }

  return (
    <SortableStickyGrid
      entities={data}
      renderGridBody={renderBody}
      gridHeaders={GRID_HEADERS}
      className="list-view"
      defaultSortProperty="start"
    />
  );
}

function RunsListView({ bucketInfo }) {
  if (!bucketInfo) {
    return (
      <div className="runs-list-container">
        <h3 className="text-center">{T.translate(`${PREFIX}.noData`)}</h3>
      </div>
    );
  }

  return (
    <div className="runs-list-container">
      <TopPanel />

      {renderGrid(bucketInfo.runsList)}
    </div>
  );
}

RunsListView.propTypes = {
  bucketInfo: PropTypes.object,
};

const mapStateToProps = (state) => {
  return {
    bucketInfo: state.dashboard.displayBucketInfo,
  };
};

const RunsList = connect(mapStateToProps)(RunsListView);

export default RunsList;
