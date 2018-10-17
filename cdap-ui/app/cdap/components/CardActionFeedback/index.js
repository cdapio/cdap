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
import isObject from 'lodash/isObject';
import IconSVG from 'components/IconSVG';

var classNames = require('classnames');
export const CARD_ACTION_TYPES = {
  SUCCESS: 'SUCCESS',
  DANGER: 'DANGER',
  WARNING: 'WARNING',
  LOADING: 'LOADING',
};

export default class CardActionFeedback extends Component {
  static propTypes = {
    type: PropTypes.oneOf([
      CARD_ACTION_TYPES.SUCCESS,
      CARD_ACTION_TYPES.WARNING,
      CARD_ACTION_TYPES.DANGER,
      CARD_ACTION_TYPES.LOADING,
    ]).isRequired,
    message: PropTypes.string,
    extendedMessage: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.shape({
        response: PropTypes.string,
      }),
    ]),
  };

  state = {
    isExpanded: false,
  };

  getIcon() {
    let iconName = '';
    let className = 'feedback-icon';
    switch (this.props.type) {
      case CARD_ACTION_TYPES.SUCCESS:
        iconName = 'icon-check';
        break;
      case CARD_ACTION_TYPES.DANGER:
        iconName = 'icon-exclamation-triangle';
        break;
      case CARD_ACTION_TYPES.WARNING:
        iconName = 'icon-exclamation-triangle';
        break;
      case CARD_ACTION_TYPES.LOADING:
        iconName = 'icon-spinner';
        className = 'fa-spin feedback-icon';
        break;
      default:
        return null;
    }
    return <IconSVG name={iconName} className={className} />;
  }

  getExtendedMessage() {
    if (this.props.extendedMessage) {
      return (
        <div className="stack-trace">
          {isObject(this.props.extendedMessage) ? (
            <pre>{this.props.extendedMessage.response}</pre>
          ) : (
            <pre>{this.props.extendedMessage}</pre>
          )}
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
            className="expand-icon float-right text-center"
            onClick={this.handleToggleExtendedMessage.bind(this)}
          >
            <IconSVG name="icon-angle-double-up" />
          </span>
        );
        extendedMessage = this.getExtendedMessage();
      } else {
        angleIcon = (
          <span
            className="expand-icon float-right text-center"
            onClick={this.handleToggleExtendedMessage.bind(this)}
          >
            <IconSVG name="icon-angle-double-down" />
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
