/*
 * Copyright © 2016 Cask Data, Inc.
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
import NamespaceStore from 'services/NamespaceStore';
import {MyProgramApi} from 'api/program';
import FastActionButton from '../FastActionButton';
import {convertProgramToApi} from 'services/program-api-converter';
import ConfirmationModal from 'components/ConfirmationModal';
import {Tooltip} from 'reactstrap';
import T from 'i18n-react';

export default class StartStopAction extends Component {
  constructor(props) {
    super(props);

    this.params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      appId: this.props.entity.applicationId,
      programType: convertProgramToApi(this.props.entity.programType),
      programId: this.props.entity.id
    };

    this.state = {
      status: 'loading',
      modal: false,
      tooltipOpen: false,
      errorMessage: '',
      extendedMessage: '',
    };

    this.startStop;
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.onClick = this.onClick.bind(this);
    this.doStartStop = this.doStartStop.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
  }

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }
  toggleModal() {
    this.setState({
      modal: !this.state.modal,
      errorMessage: '',
      extendedMessage: ''
    });
  }

  componentWillMount() {
    this.statusPoll$ = MyProgramApi.pollStatus(this.params)
      .subscribe((res) => {

        // If the fast action has stopped loading and the modal is open, and we do not have an error message, close the modal
        if (this.state.status === 'loading' && this.state.status !== res.status && this.state.modal && !this.state.errorMessage) {
          this.setState({
            status: res.status,
            modal: false
          });
        } else {
          this.setState({
            status: res.status
          });
        }
      });
  }

  componentWillUnmount() {
    this.statusPoll$.dispose();
  }

  onClick(e) {
    this.toggleModal();
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
  }

  doStartStop() {
    let params = Object.assign({}, this.params);
    if (this.state.status === 'RUNNING' || this.state.status === 'STARTING') {
      params.action = 'stop';
    } else {
      params.action = 'start';
    }

    this.setState({
      status: 'loading',
      startStop: params.action
    });

    MyProgramApi.action(params)
      .subscribe((res) => {
        this.props.onSuccess(res);
        this.setState({
          errorMessage : '',
          extendedMessage : ''
        });
      }, (err) => {
        this.setState({
          errorMessage : `Program ${this.props.entity.id} failed to ${params.action}`,
          extendedMessage : err.response,
          status: ''
        });
      });
  }


  render() {
    let icon;
    let confirmBtnText;
    let headerText;
    let confirmationText;

    if (this.state.status === 'RUNNING' || this.state.status === 'STARTING') {
      this.startStop = 'stop';
      icon = 'fa fa-stop text-danger';
      confirmBtnText = "stopConfirmLabel";
      headerText = T.translate('features.FastAction.stopProgramHeader');
      confirmationText = T.translate('features.FastAction.stopConfirmation', {entityId: this.props.entity.id});
    } else {
      this.startStop = 'start';
      icon = 'fa fa-play text-success';
      confirmBtnText = "startConfirmLabel";
      headerText = T.translate('features.FastAction.startProgramHeader');
      confirmationText = T.translate('features.FastAction.startConfirmation', {entityId: this.props.entity.id});
    }
    let tooltipID = `${this.props.entity.uniqueId}-${this.startStop}`;

    return (
      <span>
        {
          this.state.modal ? (
            <ConfirmationModal
              headerTitle={headerText}
              toggleModal={this.toggleModal}
              confirmationText={confirmationText}
              confirmButtonText={T.translate('features.FastAction.' + confirmBtnText)}
              confirmFn={this.doStartStop}
              cancelFn={this.toggleModal}
              isLoading={this.state.status === 'loading'}
              isOpen={this.state.modal}
              errorMessage={this.state.errorMessage}
              disableAction={!!this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
            />
          ) : null
        }
        {
          this.state.status === 'loading' ? (
            <button className="btn btn-link" disabled>
              <span className="fa fa-spin fa-spinner"></span>
            </button>
          ) :
          (
            <span>
              <FastActionButton
                icon={icon}
                action={this.onClick}
                id={tooltipID}
              />
              <Tooltip
                placement="top"
                isOpen={this.state.tooltipOpen}
                target={tooltipID}
                toggle={this.toggleTooltip}
                delay={0}
              >
                {T.translate(`features.FastAction.${this.startStop}`)}
              </Tooltip>
            </span>
          )
        }
      </span>
    );
  }
}

StartStopAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    applicationId: PropTypes.string.isRequired,
    programType: PropTypes.string.isRequired
  }),
  onSuccess: PropTypes.func
};
