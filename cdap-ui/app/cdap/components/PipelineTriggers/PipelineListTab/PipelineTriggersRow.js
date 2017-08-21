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

import React, { Component, PropTypes } from 'react';
import IconSVG from 'components/IconSVG';
import PipelineTriggersStore from 'components/PipelineTriggers/store/PipelineTriggersStore';
import {enableSchedule} from 'components/PipelineTriggers/store/PipelineTriggersActionCreator';
import PayloadConfigModal from 'components/PipelineTriggers/PayloadConfigModal';
import T from 'i18n-react';

const TRIGGER_PREFIX = 'features.PipelineTriggers';
const PREFIX = `${TRIGGER_PREFIX}.SetTriggers`;

export default class PipelineTriggersRow extends Component {
  static propTypes = {
    isExpanded: PropTypes.bool,
    onToggle: PropTypes.func,
    pipelineRow: PropTypes.string,
    triggeringPipelineInfo: PropTypes.object,
    triggeredPipelineInfo: PropTypes.object,
    selectedNamespace: PropTypes.string
  };

  state = {
    completed: true,
    killed: false,
    failed: false
  };

  constructor(props) {
    super(props);
    this.pipelineName = PipelineTriggersStore.getState().triggers.pipelineName;
  }

  toggleKey(key) {
    this.setState({
      [key]: !this.state[key]
    });
  }

  getConfig = () => {
    let config = {
      eventTriggers: []
    };
    if (this.state.completed) {
      config.eventTriggers.push('COMPLETED');
    }
    if (this.state.killed) {
      config.eventTriggers.push('KILLED');
    }
    if (this.state.failed) {
      config.eventTriggers.push('FAILED');
    }
    return config;
  }

  enableScheduleClick = () => {
    let config = this.getConfig();
    enableSchedule(this.props.triggeringPipelineInfo, this.pipelineName, this.props.selectedNamespace, config);
  }

  /*
    if key is triggering pipeline's run time argument use this as map
      {"runtimeArgumentKey":"runtimeArgsKey","type":"RUNTIME_ARG","namespace":"ns1","pipelineName":"p1"}
    if key is triggering pipeline's plugin property then use this as map
      {"pluginName":"name1","propertyKey":"key1","type":"PLUGIN_PROPERTY","namespace":"ns1","pipelineName":"p1"}
  */
  configureAndEnable = (mapping) => {
    const generateRuntimeMapping = () => {
      let runArgsMapping = {};
      let {selectedNamespace: namespace, triggeringPipelineInfo} = this.props;
      mapping.forEach(map => {
        let runargkey;
        if (map.key.split(':').length > 1) {
          let [pipelineName, pluginName, propertyKey] = map.key.split(':');
          runargkey = JSON.stringify({
            namespace,
            pipelineName,
            pluginName,
            propertyKey,
            type: 'PLUGIN_PROPERTY'
          });
        } else {
          runargkey = JSON.stringify({
            namespace,
            pipelineName: triggeringPipelineInfo.id,
            'runtimeArgumentKey': map.key,
            'type': 'RUNTIME_ARG'
          });
        }
        runArgsMapping[runargkey] = map.value;
      });
      return JSON.stringify(runArgsMapping);
    };
    let config = this.getConfig();
    config.properties = {
      'triggering.properties.mapping': generateRuntimeMapping()
    };
    enableSchedule(this.props.triggeringPipelineInfo, this.pipelineName, this.props.selectedNamespace, config);
  };

  render() {
    let {
      onToggle,
      pipelineRow,
      triggeringPipelineInfo,
      selectedNamespace
    } = this.props;

    if (!this.props.isExpanded) {
      return (
        <div
          className="pipeline-triggers-row"
          onClick={onToggle.bind(null, pipelineRow)}
        >
          <div className="caret-container">
            <IconSVG name="icon-caret-right" />
          </div>
          <div className="pipeline-name">
            {pipelineRow}
          </div>
        </div>
      );
    }

    let enabledButtonDisabled = !this.state.completed && !this.state.killed && !this.state.failed;

    return (
      <div className="pipeline-triggers-expanded-row">
        <div
          className="header-row"
          onClick={onToggle.bind(null, null)}
        >
          <div className="caret-container">
            <IconSVG name="icon-caret-down" />
          </div>

          <div className="pipeline-name">
            {pipelineRow}
          </div>
        </div>

        <div className="pipeline-description">
          <strong>{T.translate(`${TRIGGER_PREFIX}.description`)}: </strong>
          <span>
            {triggeringPipelineInfo.description}
          </span>
          <a href={`/pipelines/ns/${selectedNamespace}/view/${pipelineRow}`}>
            {T.translate(`${TRIGGER_PREFIX}.viewPipeline`)}
          </a>
        </div>

        <div className="helper-text">
          {T.translate(`${TRIGGER_PREFIX}.helperText`, {pipelineName: this.pipelineName})}
        </div>

        <div className="events-list">
          <div
            className="checkbox-item"
            onClick={this.toggleKey.bind(this, 'completed')}
          >
            <IconSVG name={this.state.completed ? 'icon-check-square' : 'icon-square-o'} />
            <span>{T.translate(`${TRIGGER_PREFIX}.Events.COMPLETED`)}</span>
          </div>

          <div
            className="checkbox-item"
            onClick={this.toggleKey.bind(this, 'killed')}
          >
            <IconSVG name={this.state.killed ? 'icon-check-square' : 'icon-square-o'} />
            <span>{T.translate(`${TRIGGER_PREFIX}.Events.KILLED`)}</span>
          </div>

          <div
            className="checkbox-item"
            onClick={this.toggleKey.bind(this, 'failed')}
          >
            <IconSVG name={this.state.failed ? 'icon-check-square' : 'icon-square-o'} />
            <span>{T.translate(`${TRIGGER_PREFIX}.Events.FAILED`)}</span>
          </div>
        </div>

        <div className="action-buttons-container clearfix">
          <button
            className="btn btn-primary"
            disabled={enabledButtonDisabled}
            onClick={this.enableScheduleClick}
          >
            {T.translate(`${PREFIX}.buttonLabel`)}
          </button>
          <PayloadConfigModal
            triggeringPipelineInfo={this.props.triggeringPipelineInfo}
            triggeredPipelineInfo={this.props.triggeredPipelineInfo}
            onEnableSchedule={this.configureAndEnable}
          />
        </div>
      </div>
    );
  }
}


