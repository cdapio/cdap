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
import classnames from 'classnames';
import ee from 'event-emitter';
import {preventPropagation} from 'services/helpers';

require('./CollapsibleSidebar.scss');

export default class CollapsibleSidebar extends Component {
  constructor(props) {
    super(props);

    this.state = {
      expanded: false
    };

    this.eventEmitter = ee(ee);
    this.onToggleClick = this.onToggleClick.bind(this);
    this.closeSidebar = this.closeSidebar.bind(this);

    this.eventEmitter.on('CLOSE_ALL_COLLAPSIBLE_SIDEBAR', this.closeSidebar);
  }

  componentWillUnmount() {
    this.eventEmitter.off('CLOSE_ALL_COLLAPSIBLE_SIDEBAR', this.closeSidebar);
  }

  onToggleClick() {
    let newState = !this.state.expanded;
    if (this.props.onlyOneCanOpen && newState) {
      this.eventEmitter.emit('CLOSE_ALL_COLLAPSIBLE_SIDEBAR');
    }

    if (this.props.onToggle) {
      this.props.onToggle(newState);
    }

    this.setState({expanded: newState});
  }

  closeSidebar() {
    if (this.props.onToggle) {
      this.props.onToggle(false);
    }

    this.setState({expanded: false});
  }

  render() {
    return (
      <div
        className={
          classnames('collapsible-sidebar', this.props.backdropClass, this.props.position, {
            backdrop: this.props.backdrop,
            expanded: this.state.expanded
          }
        )}
        onClick={this.closeSidebar}
      >
        <div
          className={classnames('collapsible-content', { 'show-content': this.state.expanded })}
          onClick={preventPropagation}
        >
          <div
            className={
              classnames('collapsible-toggle-tab text-xs-center text-center', this.props.toggleTabClass)
            }
            onClick={this.onToggleClick}
          >
            <span className="toggle-tab-label">
              {this.props.toggleTabLabel}
            </span>
          </div>

          <div className="collapsible-body">
            {this.props.children}
          </div>
        </div>
      </div>
    );
  }
}

CollapsibleSidebar.defaultProps = {
  backdrop: true,
  onlyOneCanOpen: true
};

CollapsibleSidebar.propTypes = {
  position: PropTypes.oneOf(['left', 'right']).isRequired,
  backdrop: PropTypes.bool,
  backdropClass: PropTypes.string,
  toggleTabLabel: PropTypes.string.isRequired,
  toggleTabClass: PropTypes.string,
  children: PropTypes.any,
  onlyOneCanOpen: PropTypes.bool,
  onToggle: PropTypes.func
};
