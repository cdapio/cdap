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
import {setScheduleError, setScheduleButtonLoading} from 'components/PipelineDetails/store/ActionCreator';
import PipelineScheduler from 'components/PipelineScheduler';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import StatusMapper from 'services/StatusMapper';
import {schedulePipeline, suspendSchedule} from 'components/PipelineConfigurations/Store/ActionCreator';
import {keyValuePairsHaveMissingValues} from 'components/KeyValuePairs/KeyValueStoreActions';
import PipelineConfigurations from 'components/PipelineConfigurations';

export default class PipelineScheduleButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    schedule: PropTypes.string,
    maxConcurrentRuns: PropTypes.number,
    pipelineName: PropTypes.string,
    scheduleStatus: PropTypes.string,
    scheduleButtonLoading: PropTypes.bool,
    scheduleError: PropTypes.string,
    runtimeArgs: PropTypes.array
  }

  state = {
    showScheduler: false,
    showConfigModeless: false,
    scheduleStatus: this.props.scheduleStatus
  };

  componentWillReceiveProps(nextProps) {
    let scheduleStatus = StatusMapper.lookupDisplayStatus(nextProps.scheduleStatus);
    if (scheduleStatus !== this.state.scheduleStatus) {
      this.setState({ scheduleStatus });
      setScheduleButtonLoading(false);
    }
  }

  toggleScheduler = () => {
    this.setState({
      showScheduler: !this.state.showScheduler
    });
  };

  toggleConfigModeless = () => {
    this.setState({
      showConfigModeless: !this.state.showConfigModeless
    });
  };

  schedulePipelineOrToggleConfig = () => {
    if (keyValuePairsHaveMissingValues(this.props.runtimeArgs)) {
      this.toggleConfigModeless();
    } else {
      schedulePipeline();
    }
  };

  renderScheduleError() {
    if (!this.props.scheduleError) {
      return null;
    }

    return (
      <Alert
        message={this.props.scheduleError}
        type='error'
        showAlert={true}
        onClose={setScheduleError.bind(null, '')}
      />
    );
  }

  renderConfigModeless() {
    if (!this.state.showConfigModeless) { return null; }

    return (
      <PipelineConfigurations
        onClose={this.toggleConfigModeless}
        isDetailView={true}
        isBatch={this.props.isBatch}
        pipelineName={this.props.pipelineName}
        action='schedule'
      />
    );
  }

  renderScheduleButton() {
    if ([StatusMapper.statusMap['SCHEDULED'], StatusMapper.statusMap['SUSPENDING']].indexOf(this.state.scheduleStatus) !== -1) {
      return (
        <div
          onClick={this.toggleScheduler}
          className="btn pipeline-action-btn pipeline-scheduler-btn"
          disabled={this.state.scheduleStatus === StatusMapper.statusMap['SUSPENDING']}
        >
          <div className="btn-container">
            {
              this.props.scheduleButtonLoading ?
                <IconSVG
                  name="icon-spinner"
                  className="fa-spin"
                />
              :
                <IconSVG
                  name="icon-runtimestarttime"
                  className="unschedule-icon"
                />
            }
            <div className="button-label">Unschedule</div>
          </div>
        </div>
      );
    }

    return (
      <div
        onClick={this.toggleScheduler}
        className={classnames("btn pipeline-action-btn pipeline-scheduler-btn", {"btn-select" : this.state.showScheduler})}
        disabled={this.state.scheduleStatus === StatusMapper.statusMap['SCHEDULING']}
      >
        <div className="btn-container">
          {
            this.props.scheduleButtonLoading ?
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            :
              <IconSVG
                name="icon-runtimestarttime"
                className="schedule-icon"
              />
          }
          <div className="button-label">Schedule</div>
        </div>
      </div>
    );
  }

  render() {
    if (!this.props.isBatch) {
      return null;
    }

    return (
      <div className={classnames("pipeline-action-container pipeline-scheduler-container", {"active" : this.state.showScheduler})}>
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
              schedulePipeline={this.schedulePipelineOrToggleConfig}
              suspendSchedule={suspendSchedule}
            />
          :
            null
        }
        {this.renderConfigModeless()}
      </div>
    );
  }
}
