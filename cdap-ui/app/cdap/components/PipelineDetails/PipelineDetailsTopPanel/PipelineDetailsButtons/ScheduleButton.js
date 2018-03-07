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
import PropTypes from 'prop-types';
import {MyPipelineApi} from 'api/pipeline';
import {fetchScheduleStatus} from 'components/PipelineDetails/store/ActionCreator';
import PipelineScheduler from 'components/PipelineScheduler';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import StatusMapper from 'services/StatusMapper';
import {GLOBALS} from 'services/global-constants';
import {getCurrentNamespace} from 'services/NamespaceStore';

export default class ScheduleButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    schedule: PropTypes.string,
    maxConcurrentRuns: PropTypes.number,
    pipelineName: PropTypes.string,
    scheduleStatus: PropTypes.string
  }

  state = {
    showScheduler: false,
    scheduleStatus: this.props.scheduleStatus,
    scheduleLoading: true,
    scheduleError: null
  };

  componentWillReceiveProps(nextProps) {
    let scheduleStatus = StatusMapper.lookupDisplayStatus(nextProps.scheduleStatus);
    if (scheduleStatus !== this.state.scheduleStatus) {
      this.setState({
        scheduleStatus,
        scheduleLoading: false
      });
    }
  }

  toggleScheduler = () => {
    this.setState({
      showScheduler: !this.state.showScheduler
    });
  }

  scheduleOrSuspend = (scheduleApi) => {
    this.setState({
      scheduleLoading: true
    });
    let params = {
      namespace: getCurrentNamespace(),
      appId: this.props.pipelineName,
      scheduleId: GLOBALS.defaultScheduleId
    };
    scheduleApi(params)
    .subscribe(() => {
      this.setState({
        scheduleLoading: false
      });
      fetchScheduleStatus(params);
    }, (err) => {
      this.setState({
        scheduleLoading: false,
        scheduleError: err.response || err
      });
    });
  }

  renderScheduleError() {
    if (!this.state.scheduleError) {
      return null;
    }

    return (
      <Alert
        message={this.state.scheduleError}
        type='error'
        showAlert={true}
        onClose={() => this.setState({
          scheduleError: null
        })}
      />
    );
  }

  renderScheduleButton() {
    if ([StatusMapper.statusMap['DEPLOYED'], StatusMapper.statusMap['SCHEDULING']].indexOf(this.state.scheduleStatus) !== -1) {
      return (
        <div
          onClick={this.toggleScheduler}
          className={classnames("btn pipeline-scheduler-btn", {"btn-select" : this.state.showScheduler})}
          disabled={this.state.scheduleStatus === StatusMapper.statusMap['SCHEDULING']}
        >
          <div className="btn-container">
            {
              this.state.scheduleLoading ?
                <IconSVG
                  name="icon-spinner"
                  className="fa-spin"
                />
              :
                (
                  <span className="double-line">
                    <IconSVG
                      name="icon-runtimestarttime"
                      className="schedule-icon"
                    />
                    <div className="button-label">Schedule</div>
                  </span>
                )
            }
          </div>
        </div>
      );
    }

    return (
      <div
        onClick={this.toggleScheduler}
        className="btn pipeline-scheduler-btn"
        disabled={this.state.scheduleStatus === StatusMapper.statusMap['SUSPENDING']}
      >
        <div className="btn-container">
          {
            this.state.scheduleLoading ?
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            :
              (
                <span className="double-line">
                  <IconSVG
                    name="icon-runtimestarttime"
                    className="unschedule-icon"
                  />
                  <div className="button-label">Unschedule</div>
                </span>
              )
          }
        </div>
      </div>
    );
  }

  render() {
    if (!this.props.isBatch) {
      return null;
    }

    return (
      <div className="pipeline-scheduler">
        {this.renderScheduleError()}
        {this.renderScheduleButton()}
        {
          this.state.showScheduler ?
            <PipelineScheduler
              schedule={this.props.schedule}
              maxConcurrentRuns={this.props.maxConcurrentRuns}
              onClose={this.toggleScheduler}
              isDetailView={true}
              pipelineName={this.props.pipelineName}
              scheduleStatus={this.state.scheduleStatus}
              schedulePipeline={this.scheduleOrSuspend.bind(this, MyPipelineApi.schedule)}
              suspendSchedule={this.scheduleOrSuspend.bind(this, MyPipelineApi.suspend)}
            />
          :
            null
        }
      </div>
    );
  }
}
