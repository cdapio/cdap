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
import {INTERVAL_OPTIONS} from 'components/PipelineScheduler/Store';
import T from 'i18n-react';

const PREFIX = 'features.PipelineScheduler.summary';

const mapStateToSummaryProps = (state) => {
  return {
    state
  };
};

const mapStateToMaxConcurrentRunsSummaryProps = (state) => {
  return {
    maxConcurrentRuns: state.maxConcurrentRuns
  };
};

const SummaryComponent = ({state}) => {
  let minuteMoment = moment().minute(state.startingAtMinute).format('mm');
  let dateOfMonthMoment = moment().date(state.dateOfMonthInterval).format('Do');
  let monthMoment = moment().month(state.monthInterval).format('MMM');

  switch (state.intervalOption) {
    case INTERVAL_OPTIONS['5MIN']:
      return <span>every 5 minutes.</span>;
    case INTERVAL_OPTIONS['10MIN']:
      return <span>every 10 minutes.</span>;
    case INTERVAL_OPTIONS['30MIN']:
      return <span>every 30 minutes.</span>;
    case INTERVAL_OPTIONS.HOURLY:
      return (
        <span>
          {
            state.hourInterval === 1 ?
              T.translate(`${PREFIX}.everyHour`)
            :
              T.translate(`${PREFIX}.everyNumHours`, {num: state.hourInterval})
          }
          {
            state.startingAtMinute === 0 ?
              T.translate(`${PREFIX}.onTheHour`)
            :
              T.translate(`${PREFIX}.numMinsPastTheHour`, {num: minuteMoment})
          }
        </span>
      );
    case INTERVAL_OPTIONS.DAILY:
      return (
        <span>
          {
            state.dayInterval === 1 ?
              T.translate(`${PREFIX}.everyDay`)
            :
              T.translate(`${PREFIX}.everyNumDays`, {num: state.dayInterval})
          }
          {T.translate(`${PREFIX}.atHourMinuteAMPM`, {
            hour: state.startingAtHour,
            min: minuteMoment,
            AMPM: state.startingAtAMPM
          })}
        </span>
      );
    case INTERVAL_OPTIONS.WEEKLY:
      return (
        <span>
          {
            state.daysOfWeekInterval.length === 7 ?
              T.translate(`${PREFIX}.everyDay`)
            :
              <span>
                {T.translate(`${PREFIX}.every`)}
                {/* need to do -1 because our backend expects Sun-Sat values as 1-7, but moment() expects 0-6 instead */}
                {state.daysOfWeekInterval.map(day => `${moment().day(parseInt(day, 10)-1).format('dddd')}, `)}
              </span>

          }
          {T.translate(`${PREFIX}.atHourMinuteAMPM`, {
            hour: state.startingAtHour,
            min: minuteMoment,
            AMPM: state.startingAtAMPM
          })}
        </span>
      );
    case INTERVAL_OPTIONS.MONTHLY:
      return (
        <span>
          {T.translate(`${PREFIX}.everyDateOfMonth`, {date: dateOfMonthMoment})}
          {T.translate(`${PREFIX}.atHourMinuteAMPM`, {
            hour: state.startingAtHour,
            min: minuteMoment,
            AMPM: state.startingAtAMPM
          })}
        </span>
      );
    case INTERVAL_OPTIONS.YEARLY:
      return (
        <span>
          {T.translate(`${PREFIX}.everyYearOn`, {
            month: monthMoment,
            dateOfMonth: dateOfMonthMoment
          })}
          {T.translate(`${PREFIX}.atHourMinuteAMPM`, {
            hour: state.startingAtHour,
            min: minuteMoment,
            AMPM: state.startingAtAMPM
          })}
        </span>
      );
  }
};

SummaryComponent.propTypes = {
  state: PropTypes.object
};

const MaxConcurrentRunsSummary = ({maxConcurrentRuns}) => {
  return (
    <span>
      {
        maxConcurrentRuns === 1 ?
          T.translate(`${PREFIX}.cannotMaxConcurrentRuns`)
        :
          T.translate(`${PREFIX}.canMaxConcurrentRuns`, {num: maxConcurrentRuns})
      }
    </span>
  );
};

MaxConcurrentRunsSummary.propTypes = {
  maxConcurrentRuns: PropTypes.number
};

const ConnectedSummaryComponent = connect(
  mapStateToSummaryProps,
  null
)(SummaryComponent);
const ConnectedMaxConcurrentRunsSummary = connect(
  mapStateToMaxConcurrentRunsSummaryProps,
  null
)(MaxConcurrentRunsSummary);

export default function Summary() {
  return (
    <div className="form-group row summary">
      <label className="col-xs-3 control-label">
        {T.translate(`${PREFIX}.label`)}
      </label>
      <div className="col-xs-8 schedule-values-container">
        <div>
          <span>{T.translate(`${PREFIX}.scheduledToRun`)}</span>
          <ConnectedSummaryComponent />
        </div>
        <div>
          <ConnectedMaxConcurrentRunsSummary />
        </div>
      </div>
    </div>
  );
}
