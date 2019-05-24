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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import PlusButtonStore from 'services/PlusButtonStore';
import PlusButtonModal from 'components/PlusButtonModal';
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
require('./ResourceCenterButton.scss');

/**
 * This module is used solely for angular side. This includes styling and stripping of context
 * information used by the PlusButton. We may nee to consider using one component for both angular
 * and react as it seems redundant.
 */
export default class ResourceCenterButton extends Component {
  eventemitter = ee(ee);
  constructor(props) {
    super(props);
    this.state = {
      showResourceCenter: false,
    };
    this.eventemitter.on(globalEvents.OPENRESOURCECENTER, this.openResourceCenter);
    this.eventemitter.on(globalEvents.CLOSERESOURCECENTER, this.closeResourceCenter);
  }
  componentDidMount() {
    this.plusButtonSubscription = PlusButtonStore.subscribe(() => {
      let modalState = PlusButtonStore.getState().modalState;
      this.setState({
        showResourceCenter: modalState,
      });
    });
  }
  componentWillUnmount() {
    if (this.plusButtonSubscription) {
      this.plusButtonSubscription();
    }
    this.eventemitter.off(globalEvents.OPENRESOURCECENTER, this.openResourceCenter);
    this.eventemitter.off(globalEvents.CLOSERESOURCECENTER, this.closeResourceCenter);
  }
  openResourceCenter = () => {
    this.setState({
      showResourceCenter: true,
    });
  };
  closeResourceCenter = () => {
    this.setState({
      showResourceCenter: false,
    });
  };
  render() {
    return (
      <div>
        <div
          className={classnames('cask-resourcecenter-button', this.props.className)}
          onClick={this.openResourceCenter.bind(this)}
        >
          <img
            id="resource-center-btn"
            className="button-container"
            src="/cdap_assets/img/plus_ico.svg"
          />
        </div>
        <PlusButtonModal
          isOpen={this.state.showResourceCenter}
          onCloseHandler={this.closeResourceCenter.bind(this)}
          mode="resourcecenter"
        />
      </div>
    );
  }
}
ResourceCenterButton.propTypes = {
  className: PropTypes.string,
};
