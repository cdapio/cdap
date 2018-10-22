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
import TimeRangePopover from 'components/Reports/Customizer/TimeRangeSelector/TimeRangePopover';
import { connect } from 'react-redux';
import moment from 'moment';
import T from 'i18n-react';

const PREFIX = 'features.Reports.Customizer.TimeRangeSelector';

require('./TimeRangeSelector.scss');

const format = 'MMM. D, YYYY h:mma';

function renderDisplay(selection, start, end) {
  let startTime, endTime;

  if (selection === 'custom') {
    startTime = moment(start).format(format);
    endTime = moment(end).format(format);
  }

  switch (selection) {
    case 'last30':
      return T.translate(`${PREFIX}.last30Minutes`);
    case 'lastHour':
      return T.translate(`${PREFIX}.lastHour`);
    case 'custom':
      return T.translate(`${PREFIX}.timeRange`, { startTime, endTime });
    default:
      return T.translate(`${PREFIX}.select`);
  }
}

function TimeRangeSelectorView({ selection, start, end }) {
  return (
    <div className="reports-time-range-selector">
      <div className="title">{T.translate(`${PREFIX}.label`)}</div>

      <div className="time-selector-value">
        <div className="time-icon">
          <TimeRangePopover />
        </div>
        {renderDisplay(selection, start, end)}
      </div>
    </div>
  );
}

TimeRangeSelectorView.propTypes = {
  selection: PropTypes.string,
  start: PropTypes.number,
  end: PropTypes.number,
};

const mapStateToProps = (state) => {
  return {
    selection: state.timeRange.selection,
    start: state.timeRange.start,
    end: state.timeRange.end,
  };
};

const TimeRangeSelector = connect(mapStateToProps)(TimeRangeSelectorView);

export default TimeRangeSelector;
