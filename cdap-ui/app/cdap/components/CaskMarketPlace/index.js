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

import TabConfig from './TabConfig';
import ConfigurableTab from '../ConfigurableTab';
import {MyCaskMarketApi} from '../../api/caskmarket';
import MarketAction from './action/market-action.js';
import find from 'lodash/find';

export default class CaskMarketPlace extends Component {
  componentWillMount () {
    MyCaskMarketApi.list()
      .subscribe((res) => {
        MarketAction.setList(res);
      }, (err) => {
        console.log('Error', err);
      });
  }

  handleTabClick(id) {
    let searchFilter = find(TabConfig.tabs, { id }).filter;
    MarketAction.setFilter(searchFilter);
  }

  render() {
    return (
      <ConfigurableTab
        tabConfig={TabConfig}
        onTabClick={this.handleTabClick.bind(this)}
      />
    );
  }
}
