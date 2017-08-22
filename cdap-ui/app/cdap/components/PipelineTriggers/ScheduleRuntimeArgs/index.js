/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import {fetchPipelineMacroDetails, resetStore} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsActions';
import ScheduleRuntimeArgsStore from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';
import ConfigurableTab from 'components/ConfigurableTab';
import TabConfig from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/TabConfig';
require('./ScheduleRuntimeArgs.scss');
require('./Tabs/ScheduleRuntimeTabStyling.scss');

export default class ScheduleRuntimeArgs extends Component {
  static propTypes = {
    onEnableSchedule: PropTypes.func,
    triggeringPipelineInfo: PropTypes.object.isRequired,
    triggeredPipelineInfo: PropTypes.object.isRequired
  };

  state = {
    configStages: {},
    macros: [],
    argsMapping: []
  };

  componentDidMount() {
    let {id: triggeringPipelineId, namespace: triggeringPipelineNS} = this.props.triggeringPipelineInfo;
    let {id: triggeredPipelineId, namespace: triggeredPipelineNS} = this.props.triggeredPipelineInfo;
    fetchPipelineMacroDetails(triggeringPipelineId, triggeringPipelineNS);
    fetchPipelineMacroDetails(triggeredPipelineId, triggeredPipelineNS, true);

    ScheduleRuntimeArgsStore.subscribe(() => {
      let {triggeringPipelineInfo, argsMapping} = ScheduleRuntimeArgsStore.getState().args;
      let {macros, configStages} = triggeringPipelineInfo;
      this.setState({macros, configStages, argsMapping});
    });
  }

  componentWillUnmount() {
    resetStore();
  }

  configureAndEnableTrigger = () => {
    let {argsMapping} = ScheduleRuntimeArgsStore.getState().args;
    if (this.props.onEnableSchedule) {
      this.props.onEnableSchedule(argsMapping);
    }
  };

  isEnableTriggerDisabled = () => {
    let {argsMapping} = ScheduleRuntimeArgsStore.getState().args;
    return argsMapping.length === 0;
  };

  render() {
    return (
      <div className="schedule-runtime-args">
        <ConfigurableTab
          tabConfig={TabConfig}
        />
        <hr />
        <button
          className="btn btn-primary pull-right"
          onClick={this.configureAndEnableTrigger}
          disabled={this.isEnableTriggerDisabled()}
        >
          Configure and Enable Trigger
        </button>
      </div>
    );
  }
}
