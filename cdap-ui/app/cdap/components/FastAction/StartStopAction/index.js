/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import NamespaceStore from 'services/NamespaceStore';
import {MyProgramApi} from 'api/program';
import FastActionButton from '../FastActionButton';
import {convertProgramToApi} from 'services/program-api-converter';
import ConfirmationModal from 'components/ConfirmationModal';
import IconSVG from 'components/IconSVG';
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
      programStatus: '',
      actionStatus: '',
      modal: false,
      tooltipOpen: false,
      errorMessage: '',
      extendedMessage: ''
    };

    this.startStop;
    this.toggleTooltip = this.toggleTooltip.bind(this);
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
        let programStatus = res.status;
        if (!this.state.modal) {
          if (programStatus === 'STOPPED') {
            this.startStop = 'start';
          } else {
            this.startStop = 'stop';
          }
        }
        this.setState({
          programStatus,
          actionStatus: ''
        });
      });
  }

  componentWillUpdate() {
    // don't update the modal if it's already open
    if (!this.state.modal) {
      if (this.state.programStatus === 'STOPPED') {
        this.startStop = 'start';
      } else {
        this.startStop = 'stop';
      }
    }
  }

  componentWillUnmount() {
    this.statusPoll$.dispose();
  }

  doStartStop() {
    let params = Object.assign({}, this.params);
    params.action = this.startStop;

    this.setState({
      actionStatus: 'loading'
    });

    MyProgramApi.action(params)
      .subscribe((res) => {
        this.setState({
          errorMessage : '',
          extendedMessage : '',
          modal: false
        });
        this.props.onSuccess(res);
      }, (err) => {
        this.setState({
          errorMessage : `Program ${this.props.entity.id} failed to ${params.action}`,
          extendedMessage : err.response,
          actionStatus: ''
        });
      });
  }


  render() {
    let confirmBtnText;
    let headerText;
    let confirmationText;
    let icon;
    let iconClass;

    if (this.startStop === 'start') {
      confirmBtnText = 'startConfirmLabel';
      headerText = T.translate('features.FastAction.startProgramHeader');
      confirmationText = T.translate('features.FastAction.startConfirmation', {entityId: this.props.entity.id});
    } else {
      confirmBtnText = 'stopConfirmLabel';
      headerText = T.translate('features.FastAction.stopProgramHeader');
      confirmationText = T.translate('features.FastAction.stopConfirmation', {entityId: this.props.entity.id});
    }
    // icon can change in the background, so using state instead of this.startStop
    if (this.state.programStatus === '') {
      icon = 'icon-spinner';
      iconClass = 'fa-spin';
    } else {
      if (this.state.programStatus === 'STOPPED') {
        icon = 'icon-play';
        iconClass = 'text-success';
      } else {
        icon = 'icon-stop';
        iconClass = 'text-danger';
      }
    }
    let tooltipID = `${this.props.entity.uniqueId}-${this.startStop}`;

    return (
      <span className="btn btn-secondary btn-sm">
        {
          this.state.modal ? (
            <ConfirmationModal
              headerTitle={headerText}
              toggleModal={this.toggleModal}
              confirmationText={confirmationText}
              confirmButtonText={T.translate('features.FastAction.' + confirmBtnText)}
              confirmFn={this.doStartStop}
              cancelFn={this.toggleModal}
              isLoading={this.state.actionStatus === 'loading'}
              isOpen={this.state.modal}
              errorMessage={this.state.errorMessage}
              disableAction={!!this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
            />
          ) : null
        }
        {
          this.state.actionStatus === 'loading' ? (
            <button className="btn btn-link" disabled>
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            </button>
          ) :
          (
            <span>
              <FastActionButton
                icon={icon}
                iconClasses={iconClass}
                action={this.toggleModal}
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
