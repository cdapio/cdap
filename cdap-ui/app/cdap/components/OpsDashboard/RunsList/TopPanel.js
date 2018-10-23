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
import T from 'i18n-react';
import moment from 'moment';

const PREFIX = 'features.OpsDashboard.RunsList';

function TopPanelView({ bucketInfo }) {
  const time = parseInt(bucketInfo.time, 10);

  const date = moment(time).format('ddd. MMM. D, YYYY');
  const hour = moment(time).format('h');
  const ampm = moment(time).format('a');

  return (
    <div className="top-panel-data">
      <div className="title">
        {T.translate(`${PREFIX}.title`, { count: bucketInfo.runsList.length })}
      </div>

      <div className="time-info">{T.translate(`${PREFIX}.timeRange`, { date, hour, ampm })}</div>
    </div>
  );
}

TopPanelView.propTypes = {
  bucketInfo: PropTypes.object,
};

const mapStateToProps = (state) => {
  return {
    bucketInfo: state.dashboard.displayBucketInfo,
  };
};

const TopPanel = connect(mapStateToProps)(TopPanelView);

export default TopPanel;
