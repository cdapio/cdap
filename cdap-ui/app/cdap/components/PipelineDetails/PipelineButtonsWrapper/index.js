/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
require('./PipelineButtonsWrapper.scss');

export default class PipelineButtonsWrapper extends Component {
  state = {
    activeButtons: []
  };

  static propTypes = {
    children: PropTypes.node
  };

  setActiveButton = (button, val) => {
    let activeButtons = [...this.state.activeButtons];
    let buttonIndex = activeButtons.indexOf(button);
    if (val && buttonIndex === -1) {
      activeButtons.push(button);
    } else if (!val && buttonIndex !== -1) {
      activeButtons.splice(buttonIndex, 1);
    }
    this.setState({
      activeButtons
    });
  };

  render() {
    let children = this.props.children;
    let childrenCount = React.Children.count(children);
    let activeButtons = this.state.activeButtons;

    return (
      <div className="pipeline-buttons-wrapper">
        {
          React.Children.map(children, (child, i) => {
            const leftSeparatorElem = () => {
              if (activeButtons.indexOf(i) !== -1 ||
                  !child.props.showLeftSeparator
              ) {
                return null;
              }
              return (
                <div className="pipeline-buttons-separator" />
              );
            };

            const middleSeparatorElem = () => {
              if (i === childrenCount - 1 ||
                  activeButtons.indexOf(i) !== -1 ||
                  activeButtons.indexOf(i + 1) !== -1
              ) {
                return null;
              }
              return (
                <div className="pipeline-buttons-separator" />
              );
            };

            const rightSeparatorElem = () => {
              if (activeButtons.indexOf(i) !== -1 ||
                  !child.props.showRightSeparator
              ) {
                return null;
              }
              return (
                <div className="pipeline-buttons-separator" />
              );
            };

            return (
              <div className="button-wrapper">
                {leftSeparatorElem()}
                {
                  React.cloneElement(child, {
                    ...child.props,
                    setActiveButton: this.setActiveButton.bind(this, i)
                  })
                }
                {middleSeparatorElem()}
                {rightSeparatorElem()}
              </div>
            );
          })
        }
      </div>
    );
  }
}
