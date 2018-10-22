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

/*
  Usage:
    <CardActionFeedback
      type='SUCCESS | WARNING | DANGER | LOADING'
      message='some feedback message'
      extendedMessage='stack trace message that will get rendered in <pre>'
    />
*/
import PropTypes from 'prop-types';
import React, { Component } from 'react';
require('./CardActionFeedback.scss');

var classNames = require('classnames');

export default class CardActionFeedback extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isExpanded: false,
    };
  }

  getIcon() {
    let icon = classNames(
      'fa',
      { 'fa-check': this.props.type === 'SUCCESS' },
      { 'fa-exclamation': this.props.type === 'DANGER' },
      { 'fa-exclamation-triangle': this.props.type === 'WARNING' },
      { 'fa-spinner fa-spin': this.props.type === 'LOADING' }
    );

    return (
      <span className="feedback-icon">
        <span className={icon} />
      </span>
    );
  }

  getExtendedMessage() {
    if (this.props.extendedMessage) {
      return (
        <div className="stack-trace">
          <pre>{this.props.extendedMessage}</pre>
        </div>
      );
    }
  }

  handleToggleExtendedMessage() {
    this.setState({ isExpanded: !this.state.isExpanded });
  }

  render() {
    let angleIcon;
    let extendedMessage;
    if (this.props.extendedMessage) {
      if (this.state.isExpanded) {
        angleIcon = (
          <span
            className="expand-icon float-xs-right text-xs-center"
            onClick={this.handleToggleExtendedMessage.bind(this)}
          >
            <span className="fa fa-angle-double-up" />
          </span>
        );
        extendedMessage = this.getExtendedMessage();
      } else {
        angleIcon = (
          <span
            className="expand-icon float-xs-right text-xs-center"
            onClick={this.handleToggleExtendedMessage.bind(this)}
          >
            <span className="fa fa-angle-double-down" />
          </span>
        );
      }
    }

    let feedbackClass = classNames('card-action-feedback', this.props.type);

    return (
      <div className={feedbackClass}>
        <div className="main-message">
          {this.getIcon()}
          <span className="message">{this.props.message}</span>
          {angleIcon}
        </div>
        {extendedMessage}
      </div>
    );
  }
}

CardActionFeedback.propTypes = {
  type: PropTypes.oneOf(['SUCCESS', 'WARNING', 'DANGER', 'LOADING']).isRequired,
  message: PropTypes.string,
  extendedMessage: PropTypes.string,
};
