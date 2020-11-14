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

import React, { Component } from 'react';
import { Provider } from 'react-redux';
import PropTypes from 'prop-types';
import PipelineDetailStore from 'components/PipelineDetails/store';
import PipelineSchedulerStore, {
  ACTIONS as PipelineSchedulerActions,
} from 'components/PipelineScheduler/Store';
import {
  setStateFromCron,
  getTimeBasedSchedule,
  setScheduleStatus,
} from 'components/PipelineScheduler/Store/ActionCreator';
import ViewSwitch from 'components/PipelineScheduler/ViewSwitch';
import ViewContainer from 'components/PipelineScheduler/ViewContainer';
import {
  setSchedule,
  setMaxConcurrentRuns,
  setOptionalProperty,
  setRunError,
} from 'components/PipelineDetails/store/ActionCreator';
import IconSVG from 'components/IconSVG';
import { getCurrentNamespace } from 'services/NamespaceStore';
import StatusMapper from 'services/StatusMapper';
import { objectQuery } from 'services/helpers';
import { MyScheduleApi } from 'api/schedule';
import T from 'i18n-react';
import { GLOBALS, CLOUD } from 'services/global-constants';
import isEmpty from 'lodash/isEmpty';
import PipelineModeless from 'components/PipelineDetails/PipelineModeless';

const PREFIX = 'features.PipelineScheduler';

require('./PipelineScheduler.scss');

export default class PipelineScheduler extends Component {
  constructor(props) {
    super(props);
    if (!this.props.isDetailView) {
      PipelineSchedulerStore.dispatch({
        type: PipelineSchedulerActions.SET_MAX_CONCURRENT_RUNS,
        payload: {
          maxConcurrentRuns: this.props.maxConcurrentRuns,
        },
      });
      setStateFromCron(this.props.schedule);
    }

    this.state = {
      isScheduleChanged: false,
      savingSchedule: false,
      savingAndScheduling: false,
      scheduleStatus: this.props.scheduleStatus,
    };
    setScheduleStatus(this.props.scheduleStatus);

    this.schedulerStoreSubscription = PipelineSchedulerStore.subscribe(() => {
      let state = PipelineSchedulerStore.getState();
      let currentCron = state.cron;
      let curretMaxConcurrentRuns = state.maxConcurrentRuns;
      let currentProfileName = state.profiles.selectedProfile;
      let currentBackendSchedule = state.currentBackendSchedule || {};
      let constraintFromBackend =
        (currentBackendSchedule.constraints || []).find((constraint) => {
          return constraint.type === 'CONCURRENCY';
        }) || {};
      let profileNameFromBackend =
        objectQuery(
          state,
          'currentBackendSchedule',
          'properties',
          CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY
        ) || null;
      if (
        currentCron !== objectQuery(currentBackendSchedule, 'trigger', 'cronExpression') ||
        curretMaxConcurrentRuns !== constraintFromBackend.maxConcurrency ||
        currentProfileName !== profileNameFromBackend
      ) {
        this.setState({
          isScheduleChanged: true,
        });
      } else {
        this.setState({
          isScheduleChanged: false,
        });
      }
    });
  }

  componentDidMount() {
    if (!this.props.isDetailView) {
      return;
    }

    getTimeBasedSchedule();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.scheduleStatus !== this.state.scheduleStatus) {
      this.setState({
        scheduleStatus: nextProps.scheduleStatus,
      });
    }
  }

  componentWillUnmount() {
    PipelineSchedulerStore.dispatch({
      type: PipelineSchedulerActions.RESET,
    });
    if (this.schedulerStoreSubscription) {
      this.schedulerStoreSubscription();
    }
    if (this.documentClick$) {
      this.documentClick$.unsubscribe();
    }
  }

  startScheduleAndClose = () => {
    if (this.state.isScheduleChanged) {
      this.saveSchedule(this.state.isScheduleChanged);
    } else {
      this.schedulePipeline();
    }
  };

  schedulePipeline = () => {
    this.props.onClose();
    this.props.schedulePipeline();
  };

  suspendSchedule = () => {
    this.props.onClose();
    this.props.suspendSchedule();
  };

  saveSchedule = (shouldSchedule = false) => {
    let {
      cron,
      maxConcurrentRuns,
      currentBackendSchedule,
      profiles,
    } = PipelineSchedulerStore.getState();

    // In Studio mode, which is still using Angular action creator
    if (!this.props.isDetailView) {
      this.props.actionCreator.setSchedule(cron);
      this.props.actionCreator.setMaxConcurrentRuns(maxConcurrentRuns);
      this.props.onClose();
      return;
    }

    let savingState = shouldSchedule ? 'savingAndScheduling' : 'savingSchedule';

    this.setState({
      [savingState]: true,
    });

    let scheduleProperties = currentBackendSchedule.properties;
    let newConstraints = currentBackendSchedule.constraints.map((constraint) => {
      if (constraint.type === 'CONCURRENCY') {
        return {
          ...constraint,
          maxConcurrency: maxConcurrentRuns,
        };
      }
      return constraint;
    });
    if (profiles.selectedProfile) {
      scheduleProperties = {
        ...scheduleProperties,
        [CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY]: profiles.selectedProfile,
      };
    }
    if (!isEmpty(profiles.profileCustomizations)) {
      let profileCustomizations = {};
      Object.keys(profiles.profileCustomizations).forEach((profileProp) => {
        profileCustomizations[`${CLOUD.PROFILE_PROPERTIES_PREFERENCE}.${profileProp}`] =
          profiles.profileCustomizations[profileProp];
      });
      scheduleProperties = {
        ...scheduleProperties,
        ...profileCustomizations,
      };
    }
    let newTrigger = {
      ...currentBackendSchedule.trigger,
      cronExpression: cron,
    };
    let newSchedule = {
      ...currentBackendSchedule,
      properties: scheduleProperties,
      constraints: newConstraints,
      trigger: newTrigger,
    };
    let { name: appId } = PipelineDetailStore.getState();

    MyScheduleApi.update(
      {
        namespace: getCurrentNamespace(),
        appId,
        scheduleName: GLOBALS.defaultScheduleId,
      },
      newSchedule
    ).subscribe(
      () => {
        if (shouldSchedule) {
          this.schedulePipeline();
        } else {
          this.setState({
            [savingState]: false,
          });
          this.props.onClose();
        }
      },
      (err) => {
        setRunError(err.response || err);
        this.setState({
          [savingState]: false,
        });
      }
    );

    setSchedule(cron);
    setMaxConcurrentRuns(maxConcurrentRuns);
    setOptionalProperty('app.deploy.update.schedules', true);
  };

  renderHeaderText() {
    return (
      T.translate(`${PREFIX}.header`) +
      (this.props.pipelineName.length ? ` ${this.props.pipelineName}` : '')
    );
  }

  renderActionButtons() {
    if (this.state.scheduleStatus === StatusMapper.statusMap['SCHEDULED']) {
      return (
        <div className="schedule-navigation">
          <button className="btn btn-primary schedule-btn" onClick={this.suspendSchedule}>
            {T.translate(`${PREFIX}.suspendSchedule`)}
          </button>
        </div>
      );
    }

    if (!this.props.isDetailView) {
      return (
        <div className="schedule-navigation">
          <button
            className="btn btn-primary save-schedule-btn"
            data-cy="save-schedule-btn-studio"
            onClick={this.saveSchedule.bind(this, false)}
          >
            <span>{T.translate(`${PREFIX}.saveSchedule`)}</span>
          </button>
        </div>
      );
    }

    return (
      <div className="schedule-navigation">
        <button
          data-cy="save-start-schedule-btn"
          className="btn btn-primary start-schedule-btn"
          onClick={this.startScheduleAndClose}
          disabled={this.state.savingSchedule || this.state.savingAndScheduling}
        >
          <span>
            {this.state.isScheduleChanged
              ? T.translate(`${PREFIX}.saveAndStartSchedule`)
              : T.translate(`${PREFIX}.startSchedule`)}
            {this.state.savingAndScheduling ? (
              <IconSVG name="icon-spinner" className="fa-spin" />
            ) : null}
          </span>
        </button>
        <button
          data-cy="save-schedule-btn"
          className="btn btn-secondary start-schedule-btn"
          onClick={this.saveSchedule.bind(this, false)}
          disabled={this.state.savingSchedule || this.state.savingAndScheduling}
        >
          <span>{T.translate(`${PREFIX}.saveSchedule`)}</span>
          {this.state.savingSchedule ? <IconSVG name="icon-spinner" className="fa-spin" /> : null}
        </button>
      </div>
    );
  }

  render() {
    let isScheduled = this.state.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    return (
      <Provider store={PipelineSchedulerStore}>
        <PipelineModeless title={this.renderHeaderText()} onClose={this.props.onClose}>
          <div
            className="pipeline-scheduler-content"
            ref={(ref) => (this.schedulerComponent = ref)}
          >
            <div className="pipeline-scheduler-body">
              <div className="schedule-content">
                <fieldset disabled={isScheduled}>
                  <ViewSwitch />
                  <ViewContainer isDetailView={this.props.isDetailView} />
                </fieldset>
                {this.renderActionButtons()}
              </div>
            </div>
          </div>
        </PipelineModeless>
      </Provider>
    );
  }
}

PipelineScheduler.propTypes = {
  actionCreator: PropTypes.object,
  schedule: PropTypes.string,
  maxConcurrentRuns: PropTypes.number,
  onClose: PropTypes.func,
  isDetailView: PropTypes.bool,
  pipelineName: PropTypes.string,
  scheduleStatus: PropTypes.string,
  schedulePipeline: PropTypes.func,
  suspendSchedule: PropTypes.func,
};
