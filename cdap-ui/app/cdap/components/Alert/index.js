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

import React, {Component, PropTypes} from 'react';
import {Modal} from 'reactstrap';
require('./Alert.scss');

export default class Alert extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showAlert: false || props.showAlert,
      message: props.message,
      type: props.type
    };
  }
  componentWillReceiveProps(nextProps) {
    let {showAlert, type, message} = nextProps;
    if (
      showAlert !== this.state.showAlert ||
      type !== this.state.type ||
      message !== this.state.message
    ) {
      this.setState({
        showAlert,
        type,
        message
      });
    }
  }
  onClose = () => {
    this.setState({
      showAlert: false,
      message: '',
      type: ''
    });
    if (this.props.onClose) {
      this.props.onClose();
    }
  };
  render() {
    return (
      <Modal
        isOpen={this.state.showAlert}
        toggle={() => {}}
        backdrop={false}
        keyboard={true}
        className="global-alert">
        <div className={this.state.type}>
          <span className="message">{this.state.message}</span>
          <span className="fa fa-times" onClick={this.onClose}></span>
        </div>
      </Modal>
    );
  }
}
Alert.propTypes = {
  showAlert: PropTypes.bool,
  message: PropTypes.string,
  onClose: PropTypes.func,
  type: PropTypes.oneOf([
    'success',
    'error',
    'info'
  ])
};
