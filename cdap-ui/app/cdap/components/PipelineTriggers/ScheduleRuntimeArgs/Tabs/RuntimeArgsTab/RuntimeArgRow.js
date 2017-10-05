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
import ScheduleRuntimeArgsStore, {DEFAULTRUNTIMEARGSMESSAGE, DEFAULTTRIGGEREDMACROMESSAGE} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsStore';
import {setArgMapping} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsActions';
import {Row, Col} from 'reactstrap';
import T from 'i18n-react';

export default class RuntimeArgRow extends Component {
  static propTypes = {
    mkey: PropTypes.string,
    mvalue: PropTypes.string
  };

  state = {
    key: this.props.mkey,
    value: this.props.mvalue
  };

  onKeyChange = (e) => {
    this.setState({
      key: e.target.value
    }, this.updateStore.bind(this, this.state.value));
  };

  onValueChange = (e) => {
    let oldValue = this.state.value;
    this.setState({
      value: e.target.value
    }, this.updateStore.bind(this, oldValue));
  };

  updateStore = (oldValue) => {
    if (this.state.value && this.state.key) {
      setArgMapping(this.state.key, this.state.value, 'runtime', oldValue);
    }
  };
  getDisplayValueForTriggeringPipelineMacro = (triggeringPipelineInfo, key = this.state.key) => {
    if (triggeringPipelineInfo.macros.indexOf(key) === -1) {
      return [DEFAULTRUNTIMEARGSMESSAGE];
    }
    return key ? [key, DEFAULTRUNTIMEARGSMESSAGE] : [DEFAULTRUNTIMEARGSMESSAGE];
  };

  getDisplayValueForTriggeredPipelineMacro = (triggeredPipelineInfo, triggeringPipelineInfo, value = this.state.value) => {
    if (triggeredPipelineInfo.macros.indexOf(value) === -1) {
      return [DEFAULTTRIGGEREDMACROMESSAGE];
    }
    return value ? [value, DEFAULTTRIGGEREDMACROMESSAGE] : [DEFAULTTRIGGEREDMACROMESSAGE];
  };

  render() {
    let {triggeringPipelineInfo, triggeredPipelineInfo} = ScheduleRuntimeArgsStore.getState().args;

    return (
      <Row>
        <Col xs={6}>
          <div className="select-dropdown">
            <select
              value={this.state.key}
              onChange={this.onKeyChange}
            >
              {
                this.getDisplayValueForTriggeringPipelineMacro(triggeringPipelineInfo)
                  .concat(triggeringPipelineInfo.macros)
                  .map((macro) => {
                    return (
                      <option
                        key={macro}
                        value={macro}
                      >
                        {macro}
                      </option>
                    );
                  })
              }
            </select>
          </div>
        </Col>
        <Col xs={1}>
          <span>{T.translate('commons.as')}</span>
        </Col>
        <Col xs={5}>
          <div className="select-dropdown">
            <select
              value={this.state.value}
              onChange={this.onValueChange}
            >
              {
                this.getDisplayValueForTriggeredPipelineMacro(triggeredPipelineInfo, triggeringPipelineInfo)
                  .concat(triggeredPipelineInfo.unMappedMacros)
                  .map((macro) => {
                    return (
                      <option
                        key={macro}
                        value={macro}
                      >
                        {macro}
                      </option>);
                  })
              }
            </select>
          </div>
        </Col>
      </Row>
    );
  }
}
