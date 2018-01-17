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
import classnames from 'classnames';
import {SCHEDULE_VIEWS, ACTIONS as PipelineSchedulerActions} from 'components/PipelineScheduler/Store';
import {setStateFromCron} from 'components/PipelineScheduler/Store/ActionCreator';

const mapStateToBasicViewSwitchProps = (state) => {
  return {
    scheduleView: state.scheduleView
  };
};

const mapDispatchToBasicViewSwitchProps = (dispatch) => {
  return {
    onClick: () => {
      setStateFromCron();
      dispatch({
        type: PipelineSchedulerActions.SET_SCHEDULE_VIEW,
        payload: {
          scheduleView: SCHEDULE_VIEWS.BASIC
        }
      });
    }
  };
};

const mapStateToAdvancedViewSwitchProps = (state) => {
  return {
    scheduleView: state.scheduleView
  };
};

const mapDispatchToAdvancedViewSwitchProps = (dispatch) => {
  return {
    onClick: () => {
      dispatch({
        type: PipelineSchedulerActions.SET_SCHEDULE_VIEW,
        payload: {
          scheduleView: SCHEDULE_VIEWS.ADVANCED
        }
      });
    }
  };
};

const BasicViewSwitch = ({scheduleView, onClick}) => {
  return (
    <span
      className={classnames('col-xs-3 schedule-type schedule-type-basic', { 'active': scheduleView === SCHEDULE_VIEWS.BASIC })}
      onClick={onClick}
    >
      Basic
    </span>
  );
};

BasicViewSwitch.propTypes = {
  scheduleView: PropTypes.string,
  onClick: PropTypes.func
};

const AdvancedViewSwitch = ({scheduleView, onClick}) => {
  return (
    <span
      className={classnames('col-xs-4 schedule-type schedule-type-advanced', { 'active': scheduleView === SCHEDULE_VIEWS.ADVANCED })}
      onClick={onClick}
    >
      Advanced
    </span>
  );
};

AdvancedViewSwitch.propTypes = {
  scheduleView: PropTypes.string,
  onClick: PropTypes.func
};

const ConnectedBasicViewSwitch = connect(
  mapStateToBasicViewSwitchProps,
  mapDispatchToBasicViewSwitchProps
)(BasicViewSwitch);

const ConnectedAdvancedViewSwitch = connect(
  mapStateToAdvancedViewSwitchProps,
  mapDispatchToAdvancedViewSwitchProps
)(AdvancedViewSwitch);

export default function ViewSwitch() {
  return (
    <div className="schedule-types row">
      <ConnectedBasicViewSwitch />
      <span className="separator">
        |
      </span>
      <ConnectedAdvancedViewSwitch />
    </div>
  );
}
