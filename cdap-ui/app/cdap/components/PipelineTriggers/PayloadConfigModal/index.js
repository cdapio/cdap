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
import { ModalHeader, ModalBody } from 'reactstrap';
import HydratorModal from 'components/HydratorModal';
import ScheduleRuntimeArgs from 'components/PipelineTriggers/ScheduleRuntimeArgs';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';

require('./PayloadConfigModal.scss');

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs.PayloadConfigModal';

export default class PayloadConfigModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    triggeringPipelineInfo: PropTypes.object,
    triggeredPipelineInfo: PropTypes.string,
    onClose: PropTypes.func,
    onEnableSchedule: PropTypes.func,
    disabled: PropTypes.bool,
    scheduleInfo: PropTypes.object
  };

  state = {
    isOpen: false
  };

  toggle = () => {
    this.setState({
      isOpen: !this.state.isOpen
    });
    if (!this.state.isOpen && this.props.onClose) {
      this.props.onClose();
    }
  };

  renderModal = () => {
    if (!this.state.isOpen) {
      return null;
    }

    return (
      <HydratorModal
        isOpen={this.state.isOpen}
        toggle={this.toggle}
        modalClassName="payload-config-modal"
        size="lg"
        zIndex="1061"
      >
        <ModalHeader className="clearfix">
          <span className="pull-left">
            {T.translate(`${PREFIX}.title`)}
          </span>
          <div className="btn-group pull-right">
            <a
              className="btn"
              onClick={this.toggle}
            >
              <IconSVG
                name="icon-close"
                className="fa"
              />
            </a>
          </div>
        </ModalHeader>
        <ModalBody>
          <ScheduleRuntimeArgs
            triggeringPipelineInfo={this.props.triggeringPipelineInfo}
            triggeredPipelineInfo={this.props.triggeredPipelineInfo}
            onEnableSchedule={this.props.onEnableSchedule}
            disabled={this.props.disabled}
            scheduleInfo={this.props.scheduleInfo}
          />
        </ModalBody>
      </HydratorModal>
    );
  };

  render() {
    let label = this.props.disabled ? T.translate(`${PREFIX}.configPayloadBtnDisabled`) : T.translate(`${PREFIX}.configPayloadBtn`);

    return (
      <div className="payload-config-modal">
        <button
          className="btn btn-link"
          onClick={this.toggle}
        >
          {label}
        </button>
        {this.renderModal()}
      </div>
    );
  }
}
