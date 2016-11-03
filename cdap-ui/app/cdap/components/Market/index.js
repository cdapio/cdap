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
import {MyMarketApi} from '../../api/market';
import MarketAction from './action/market-action.js';
import find from 'lodash/find';
import MarketStore from 'components/Market/store/market-store.js';

export default class Market extends Component {
  componentWillMount () {
    MyMarketApi.list()
      .subscribe((res) => {
        MarketAction.setList(res);
      }, (err) => {
        MarketAction.setError();
        console.log('Error', err);
      });
  }

  handleTabClick(id) {
    let searchFilter = find(TabConfig.tabs, { id }).filter;
    MarketAction.setFilter(searchFilter);
  }

  componentWillUnmount() {
    MarketStore.dispatch({type: 'RESET'});
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
