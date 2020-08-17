/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import { Modal } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import { ALERT_STATUS } from 'services/AlertStatus';

require('./Alert.scss');
const CLOSE_TIMEOUT = 3000;

export default class Alert extends Component {
  state = {
    showAlert: this.props.showAlert || false,
    message: this.props.message,
    element: this.props.element,
    type: this.props.type,
  };

  static propTypes = {
    showAlert: PropTypes.bool,
    message: PropTypes.string,
    element: PropTypes.node,
    onClose: PropTypes.func,
    type: PropTypes.oneOf(['success', 'error', 'info']),
  };

  alertTimeout = null;

  componentDidMount() {
    this.resetTimeout();
  }

  componentWillReceiveProps(nextProps) {
    let { showAlert, type, message, element } = nextProps;
    if (
      showAlert !== this.state.showAlert ||
      type !== this.state.type ||
      message !== this.state.message
    ) {
      this.setState({
        showAlert,
        type,
        message,
        element,
      });
    }
    this.resetTimeout();
  }

  resetTimeout = () => {
    if (this.state.type === ALERT_STATUS.Success || this.state.type === ALERT_STATUS.Info) {
      clearTimeout(this.alertTimeout);
      this.alertTimeout = setTimeout(this.onClose, CLOSE_TIMEOUT);
    }
  };

  onClose = () => {
    this.setState({
      showAlert: false,
      message: '',
      element: null,
      type: '',
    });
    if (this.props.onClose) {
      this.props.onClose();
    }
  };

  render() {
    let msgElem = null;
    if (this.state.element) {
      msgElem = <span className="message truncate">{this.state.element}</span>;
    } else if (this.state.message) {
      let message = this.state.message;
      if (typeof message !== 'string') {
        message = JSON.stringify(message);
      }

      msgElem = (
        <span className="message truncate" title={this.state.message}>
          {message}
        </span>
      );
    }
    return (
      <Modal
        isOpen={this.state.showAlert}
        toggle={() => {}}
        backdrop={false}
        keyboard={true}
        className="global-alert"
        zIndex={1061 /* This is required for showing error in angular side*/}
      >
        <div className={this.state.type}>
          {msgElem}
          <IconSVG name="icon-close" onClick={this.onClose} />
        </div>
      </Modal>
    );
  }
}
