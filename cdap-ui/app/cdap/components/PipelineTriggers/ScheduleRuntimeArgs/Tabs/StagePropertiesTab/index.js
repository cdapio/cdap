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

import React from 'react';
import ScheduleRuntimeArgsStore, {DEFAULTFIELDDELIMITER} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';
import StagePropertiesRow from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/StagePropertiesTab/StagePropertiesRow';
import {Row, Col} from 'reactstrap';
import difference from 'lodash/difference';

export default function StagePropertiesTab() {
  let {triggeringPipelineInfo, triggeredPipelineInfo, argsMapping} = ScheduleRuntimeArgsStore.getState().args;
  let macrosWithConfigProperty = argsMapping.filter(arg => arg.key && arg.key.split(DEFAULTFIELDDELIMITER).length > 1).map(arg => arg.value);
  return (
    <div className="stage-properties-tab">
      <h4>
        Set which of the plugin properties in trigger "{triggeringPipelineInfo.id}" map to "{triggeredPipelineInfo.id}" Runtime Arguments.
        <br />
        <small>(if not mapped, Runtime Arguments are derived from pipeline's or namespace's preferences)</small>
      </h4>
      {
        !triggeredPipelineInfo.macros.length ?
          <div className="empty-message-container">
            <h4> No Runtime Arguments found for "{triggeredPipelineInfo.id}"</h4>
          </div>
        :
        <div>
          <Row className="header">
            <Col xs={3}>
              Plugin Name
            </Col>
            <Col xs={4}>
              Plugin  Property
            </Col>
            <Col xs={1}>
            </Col>
            <Col xs={4}>
              Runtime Argument
            </Col>
          </Row>

          {
            // Pull all those macros with config stages in its key to the top instead of hiding it somewhere in the bottom.
            macrosWithConfigProperty
            .concat(difference(triggeredPipelineInfo.macros, macrosWithConfigProperty))
            .map(macro => {
              let keySplit = [];
              let pipelineName, pipelineStage, stageProperty, value;
              let matchingKeyValue = argsMapping.find(arg => arg.value === macro);
              if (matchingKeyValue && matchingKeyValue.key && matchingKeyValue.key.split(DEFAULTFIELDDELIMITER).length > 1) {
                keySplit = matchingKeyValue.key.split(DEFAULTFIELDDELIMITER);
                [pipelineName, pipelineStage, stageProperty] = keySplit;
                value = matchingKeyValue.value;
              }

              return (
                <StagePropertiesRow
                  pipelineName={pipelineName}
                  pipelineStage={pipelineStage}
                  stageProperty={stageProperty}
                  triggeredPipelineMacro={value}
                />
              );
            })
          }
        </div>
      }
    </div>
  );
}
