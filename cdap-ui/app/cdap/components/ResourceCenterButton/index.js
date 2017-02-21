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
import PlusButtonModal from 'components/PlusButtonModal';
import PlusButtonStore from 'services/PlusButtonStore';
import classnames from 'classnames';
require('./ResourceCenterButton.scss');

export default class ResourceCenterButton extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showResourceCenter: false
    };
  }
  componentDidMount() {
    this.plusButtonSubscription = PlusButtonStore.subscribe(() => {
      let modalState = PlusButtonStore.getState().modalState;
      this.setState({
        showResourceCenter: modalState
      });
    });
  }
  componentWillUnmount() {
    this.plusButtonSubscription();
  }
  onClickHandler() {
    this.setState({
      showResourceCenter: !this.state.showResourceCenter
    });
  }
  render() {
    return (
      <div
        className={classnames("cask-resourcecenter-button", this.props.className)}
        onClick={this.onClickHandler.bind(this)}
      >
        <div className="button-container">
          <h1>+</h1>
        </div>
        <PlusButtonModal
          isOpen={this.state.showResourceCenter}
          onCloseHandler={this.onClickHandler.bind(this)}
          mode="resourcecenter"
        />
    </div>
    );
  }
}
ResourceCenterButton.propTypes = {
  className: PropTypes.string
};
