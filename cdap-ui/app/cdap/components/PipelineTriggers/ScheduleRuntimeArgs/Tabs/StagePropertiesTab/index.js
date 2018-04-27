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
import T from 'i18n-react';
import classnames from 'classnames';

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs.Tabs.StageProps';

export default function StagePropertiesTab() {
  let {triggeringPipelineInfo, triggeredPipelineInfo, argsMapping, disabled} = ScheduleRuntimeArgsStore.getState().args;
  let macrosWithConfigProperty = argsMapping.filter(arg => arg.type === 'properties').map(arg => arg.value);

  let list = macrosWithConfigProperty;

  if (!disabled) {
    list = list.concat(difference(triggeredPipelineInfo.macros, macrosWithConfigProperty));
  }

  let emptyMessage = disabled ? `${PREFIX}.disabledNoStageConfigMessage` : `${PREFIX}.noRuntimeArgsMessage`;

  return (
    <div className="stage-properties-tab">
      {
        disabled ?
          null
        :
          (
            <h4>
              {
                T.translate(`${PREFIX}.tab_message`, {
                  triggeringPipelineid: triggeringPipelineInfo.id,
                  triggeredPipelineid: triggeredPipelineInfo.id
                })
              }
              <br />
              <small>{T.translate(`${PREFIX}.tab_message2`)}</small>
            </h4>
          )
      }
      {
        !list.length ?
          <div className={classnames("empty-message-container", {
            'margin-top-offset': !disabled
          })}>
            <h4>
              {
                T.translate(`${emptyMessage}`, {
                  triggeredPipelineid: triggeredPipelineInfo.id
                })
              }
            </h4>
          </div>
        :
        <div>
          <Row className="header">
            <Col xs={3}>
              {T.translate(`${PREFIX}.TableHeaders.pluginName`)}
            </Col>
            <Col xs={4}>
              {T.translate(`${PREFIX}.TableHeaders.pluginProperty`)}
            </Col>
            <Col xs={1}>
            </Col>
            <Col xs={4}>
              {T.translate(`${PREFIX}.TableHeaders.runtimeArg`)}
            </Col>
          </Row>

          {
            // Pull all those macros with config stages in its key to the top instead of hiding it somewhere in the bottom.
            list
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
