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
  switch (state.intervalOption) {
    case INTERVAL_OPTIONS['5MIN']:
      return <span>every 5 minutes</span>;
    case INTERVAL_OPTIONS['10MIN']:
      return <span>every 10 minutes</span>;
    case INTERVAL_OPTIONS['30MIN']:
      return <span>every 30 minutes</span>;
    case INTERVAL_OPTIONS.HOURLY:
      return (
        <span>
          every
          {
            state.hourInterval === 1 ?
              ' hour, '
            :
              ` ${state.hourInterval} hours, `
          }
          {
            state.startingAtMinute === 0 ?
              'on the hour'
            :
              `${moment().minute(state.startingAtMinute).format('mm')} minutes past the hour`
          }
        </span>
      );
    case INTERVAL_OPTIONS.DAILY:
      return (
        <span>
          {
            state.dayInterval === 1 ?
              'everyday, '
            :
              `every ${state.dayInterval} days, `
          }
          {`at ${state.startingAtHour}:${moment().minute(state.startingAtMinute).format('mm')}${state.startingAtAMPM}`}
        </span>
      );
    case INTERVAL_OPTIONS.WEEKLY:
      return (
        <span>
          {
            state.daysOfWeekInterval.length === 7 ?
              'everyday, '
            :
              <span>
                {`every `}
                {/* need to do -1 because our backend expects Sun-Sat values as 1-7, but moment() expects 0-6 instead */}
                {state.daysOfWeekInterval.map(day => `${moment().day(parseInt(day, 10)-1).format('dddd')}, `)}
              </span>

          }
          {`at ${state.startingAtHour}:${moment().minute(state.startingAtMinute).format('mm')}${state.startingAtAMPM}`}
        </span>
      );
    case INTERVAL_OPTIONS.MONTHLY:
      return (
        <span>
          {`every ${moment().date(state.dateOfMonthInterval).format('Do')} day of the month, `}
          {`at ${state.startingAtHour}:${moment().minute(state.startingAtMinute).format('mm')}${state.startingAtAMPM}`}
        </span>
      );
    case INTERVAL_OPTIONS.YEARLY:
      return (
        <span>
          {`every year on ${moment().month(state.monthInterval).format('MMM')} ${moment().date(state.dateOfMonthInterval).format('Do')}`}
          {` at ${state.startingAtHour}:${moment().minute(state.startingAtMinute).format('mm')}${state.startingAtAMPM}`}
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
          'The pipeline cannot have concurrent runs'
        :
          `The pipeline can have ${maxConcurrentRuns} concurrent runs`
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
        Summary
      </label>
      <div className="col-xs-8 schedule-values-container">
        <div>
          <span>This pipeline is scheduled to run </span>
          <ConnectedSummaryComponent />
        </div>
        <div>
          <ConnectedMaxConcurrentRunsSummary />
        </div>
      </div>
    </div>
  );
}
