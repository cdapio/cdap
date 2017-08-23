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

import React, {Component} from 'react';
import ScheduleRuntimeArgsStore, {DEFAULTFIELDDELIMITER} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';
import {Row, Col} from 'reactstrap';
import RuntimeArgRow from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/RuntimeArgsTab/RuntimeArgRow';

export default class RuntimArgsTab extends Component {

  getRuntimeArgMapping = () => {
    let {argsMapping} = ScheduleRuntimeArgsStore.getState().args;
    let runTimeArgMapping = argsMapping.filter(arg => {
      if (!arg.key || !arg.value) {
        return false;
      }
      let splitKey = arg.key.split(DEFAULTFIELDDELIMITER);
      return splitKey.length > 1 ? false : true;
    });
    return runTimeArgMapping;
  };

  state = {
    runTimeArgMapping: this.getRuntimeArgMapping()
  };

  componentDidMount() {
    ScheduleRuntimeArgsStore.subscribe(() => {
      this.setState({
        runTimeArgMapping: this.getRuntimeArgMapping()
      });
    });
  }
  renderContent() {
    let {triggeredPipelineInfo} = ScheduleRuntimeArgsStore.getState().args;
    if (!triggeredPipelineInfo.macros.length) {
      return (
        <div className="empty-message-container">
          <h4>
            No Runtime Arguments found for "{triggeredPipelineInfo.id}"
          </h4>
        </div>
      );
    }
    return (
      <div>
        <Row className="header">
          <Col xs={7}> Trigger Runtime Arguments </Col>
          <Col xs={5}> Runtime Arguments </Col>
        </Row>
        {
          triggeredPipelineInfo.macros.map((macro) => {
            let matchingKeyValue = this.state.runTimeArgMapping.find(arg => arg.value === macro);
            let key, value;
            if (matchingKeyValue) {
              key = matchingKeyValue.key.split(DEFAULTFIELDDELIMITER).length > 1 ? null : matchingKeyValue.key;
              value = matchingKeyValue.key.split(DEFAULTFIELDDELIMITER).length > 1 ? null : matchingKeyValue.value;
            }

            return (
              <RuntimeArgRow
                mkey={key}
                mvalue={value}
              />
            );

          })
        }
      </div>
    );
  }

  render() {
    let {triggeringPipelineInfo, triggeredPipelineInfo} = ScheduleRuntimeArgsStore.getState().args;
    return (
      <div className="run-time-args-tab">
        <h4>
          Select how Runtime Arguments for trigger "{triggeringPipelineInfo.id}" map to Runtime Arguments for "{triggeredPipelineInfo.id}"
          <br />
          <small>
            (if not mapped, Runtime Arguments are derived from pipeline's or namespace's preferences)
          </small>
        </h4>
        {this.renderContent()}
      </div>
    );
  }
}
