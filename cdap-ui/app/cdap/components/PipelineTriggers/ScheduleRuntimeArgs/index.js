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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {fetchPipelineMacroDetails, resetStore, bulkSetArgMapping} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsActions';
import ScheduleRuntimeArgsStore, {SCHEDULERUNTIMEARGSACTIONS} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';
import ConfigurableTab from 'components/ConfigurableTab';
import TabConfig from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/TabConfig';
import T from 'i18n-react';
import classnames from 'classnames';
import {objectQuery} from 'services/helpers';

require('./ScheduleRuntimeArgs.scss');
require('./Tabs/ScheduleRuntimeTabStyling.scss');

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs';

export default class ScheduleRuntimeArgs extends Component {
  static propTypes = {
    onEnableSchedule: PropTypes.func,
    triggeringPipelineInfo: PropTypes.object.isRequired,
    triggeredPipelineInfo: PropTypes.object.isRequired,
    disabled: PropTypes.bool,
    scheduleInfo: PropTypes.object
  };

  state = {
    configStages: {},
    macros: [],
    argsMapping: []
  };

  componentDidMount() {
    this.sub = ScheduleRuntimeArgsStore.subscribe(() => {
      let {triggeringPipelineInfo, argsMapping} = ScheduleRuntimeArgsStore.getState().args;
      let {macros, configStages} = triggeringPipelineInfo;

      this.setState({macros, configStages, argsMapping});
    });

    let {id: triggeringPipelineId, namespace: triggeringPipelineNS} = this.props.triggeringPipelineInfo;
    let {id: triggeredPipelineId, namespace: triggeredPipelineNS} = this.props.triggeredPipelineInfo;
    fetchPipelineMacroDetails(triggeringPipelineId, triggeringPipelineNS);
    fetchPipelineMacroDetails(triggeredPipelineId, triggeredPipelineNS, true);

    if (this.props.disabled) {
      ScheduleRuntimeArgsStore.dispatch({
        type: SCHEDULERUNTIMEARGSACTIONS.SETDISABLED
      });

      let {scheduleInfo} = this.props;

      let triggerProperties = objectQuery(scheduleInfo, 'properties', 'triggering.properties.mapping');

      try {
        triggerProperties = JSON.parse(triggerProperties);
      } catch (e) {
        console.log('properties are not JSON');
        return;
      }

      let argsArray = [];

      let runtimeArgs = triggerProperties.arguments;
      if (runtimeArgs.length > 0) {
        runtimeArgs.forEach((args) => {
          argsArray.push({
            key: args.source,
            value: args.target,
            type: 'runtime'
          });
        });
      }

      let pluginProperties = triggerProperties.pluginProperties;
      if (pluginProperties.length > 0) {
        pluginProperties.forEach((properties) => {
          let key = `${this.props.triggeringPipelineInfo.id}:${properties.stageName}:${properties.source}`;

          argsArray.push({
            key,
            value: properties.target,
            type: 'properties'
          });
        });
      }

      bulkSetArgMapping(argsArray);
    }
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
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
    const button = (
      <div>
        <hr />
        <button
          className="btn btn-primary pull-right"
          onClick={this.configureAndEnableTrigger}
          disabled={this.isEnableTriggerDisabled()}
        >
          {T.translate(`${PREFIX}.configure_enable_btn`)}
        </button>
      </div>
    );

    return (
      <div className={classnames('schedule-runtime-args', { disabled: this.props.disabled })}>
        <fieldset disabled={this.props.disabled}>
          <ConfigurableTab
            tabConfig={TabConfig}
          />
        </fieldset>

        {this.props.disabled ? null : button}
      </div>
    );
  }
}
