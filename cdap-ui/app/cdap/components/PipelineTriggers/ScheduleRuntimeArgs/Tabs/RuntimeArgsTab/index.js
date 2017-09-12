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
import T from 'i18n-react';

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs.Tabs.RuntimeArgs';

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

  renderEnabledRow(list) {
    return (
      list.map((macro) => {
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
    );
  }

  renderDisabledRows(list) {
    return (
      list.map((arg) => {
        return (
          <RuntimeArgRow
            mkey={arg.key}
            mvalue={arg.value}
          />
        );
      })
    );
  }

  renderContent() {
    let {triggeredPipelineInfo, disabled, argsMapping} = ScheduleRuntimeArgsStore.getState().args;
    let list = triggeredPipelineInfo.macros;

    if (disabled) {
      list = argsMapping.filter((arg) => arg.type === 'runtime');
    }

    if (!list.length) {
      let emptyMessage = disabled ? `${PREFIX}.disabledNoRuntimeArgsMessage` : `${PREFIX}.noRuntimeArgsMessage`;

      return (
        <div className="empty-message-container">
          <h4>
            {
              T.translate(`${emptyMessage}`, {
                triggeredPipelineid: triggeredPipelineInfo.id
              })
            }
          </h4>
        </div>
      );
    }

    return (
      <div>
        <Row className="header">
          <Col xs={6}> {T.translate(`${PREFIX}.TableHeaders.t_runtimeargs`)} </Col>
          <Col xs={1} />
          <Col xs={5}> {T.translate(`${PREFIX}.TableHeaders.runtimeargs`)} </Col>
        </Row>
        {disabled ? this.renderDisabledRows(list) : this.renderEnabledRow(list)}
      </div>
    );
  }

  render() {
    let {triggeringPipelineInfo, triggeredPipelineInfo, disabled} = ScheduleRuntimeArgsStore.getState().args;

    return (
      <div className="run-time-args-tab">
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
                <small>
                  {T.translate(`${PREFIX}.tab_message2`)}
                </small>
              </h4>
            )
        }
        {this.renderContent()}
      </div>
    );
  }
}
