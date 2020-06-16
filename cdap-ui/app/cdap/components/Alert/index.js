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

import IconSVG from 'components/IconSVG';
import PropTypes from 'prop-types';
import React, { Component } from 'react';
import { Modal } from 'reactstrap';

require('./Alert.scss');
const SUCCESS_CLOSE_TIMEOUT = 3000;

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

  successTimeout = null;

  componentDidMount() {
    if (this.state.type === 'success') {
      clearTimeout(this.successTimeout);
      this.successTimeout = setTimeout(this.onClose, SUCCESS_CLOSE_TIMEOUT);
    }
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
    if (this.state.type === 'success') {
      clearTimeout(this.successTimeout);
      this.successTimeout = setTimeout(this.onClose, SUCCESS_CLOSE_TIMEOUT);
    }
  }

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
