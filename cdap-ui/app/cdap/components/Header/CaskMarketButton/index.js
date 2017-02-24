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
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

export default class CaskMarketButton extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showMarketPlace: false
    };

    this.closeCaskMarketModal = this.closeCaskMarketModal.bind(this);
    this.eventEmitter = ee(ee);
    this.eventEmitter.on(globalEvents.CLOSEMARKET, this.closeCaskMarketModal);
  }

  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.CLOSEMARKET, this.closeCaskMarketModal);
  }

  onClickHandler() {
    this.setState({
      showMarketPlace: !this.state.showMarketPlace
    });
  }

  closeCaskMarketModal () {
    this.setState({
      showMarketPlace: false
    });
  }

  render() {
    return (
      <span
        className={classnames("cask-market-button", this.props.className, {'active': this.state.showMarketPlace})}
        onClick={this.onClickHandler.bind(this)}
      >
        {this.props.children}
        <PlusButtonModal
          isOpen={this.state.showMarketPlace}
          onCloseHandler={this.onClickHandler.bind(this)}
        />
    </span>
    );
  }
}
CaskMarketButton.propTypes = {
  className: PropTypes.string,
  children: PropTypes.node.isRequired
};
