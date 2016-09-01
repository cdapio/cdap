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

import React, {Component} from 'react';
import CaskMarketPlace from '../CaskMarketPlace';

export default class PlusButton extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showMarketPlace: false
    };
  }
  onClickHandler() {
    this.setState({
      showMarketPlace: !this.state.showMarketPlace
    });
  }
  render() {
    return (
      <div>
        <span
          className="fa fa-plus-circle text-success"
          onClick={this.onClickHandler.bind(this)}
        ></span>
        <CaskMarketPlace
          isOpen={this.state.showMarketPlace}
          onCloseHandler={this.onClickHandler.bind(this)}
        />
      </div>
    );
  }
}
