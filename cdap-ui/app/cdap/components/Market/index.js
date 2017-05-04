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

import {MyMarketApi} from 'api/market';
import MarketAction from './action/market-action.js';
import find from 'lodash/find';
import MarketStore from 'components/Market/store/market-store.js';
import sortedUniq from 'lodash/sortedUniq';
import cloneDeep from 'lodash/cloneDeep';
import MarketCategoriesIconMap from 'services/market-category-icon-map';

import shortid from 'shortid';

export default class Market extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tabsList: [],
      tabConfig: cloneDeep(TabConfig)
    };
  }
  componentWillMount () {
    MyMarketApi.list()
      .subscribe((res) => {
        MarketAction.setList(res);
      }, (err) => {
        MarketAction.setError();
        console.log('Error', err);
      });
      this.marketStoreSubscription = MarketStore.subscribe(() => {
        let state = MarketStore.getState();
        let tabConfig = this.state.tabConfig;
        let tabsList = sortedUniq(state.list.map(a => a.categories).reduce((prev, curr) => prev.concat(curr), []) || []);

        let tabCategoriesFromConfig = tabConfig.tabs.map(tab => tab.filter);
        let missingTabsFromConfig = sortedUniq(tabsList.filter(tab => tabCategoriesFromConfig.indexOf(tab) === -1));
        const getDefaultTabConfig = (missingTab) => {
          let placeholderIcon = MarketCategoriesIconMap[missingTab] || `icon-${missingTab[0].toUpperCase()}`;
          return {
            id: shortid.generate(),
            icon: `fa ${placeholderIcon} fa-fw`,
            content: TabConfig.defaultTabContent
          };
        };
        if (missingTabsFromConfig.length) {
          missingTabsFromConfig = missingTabsFromConfig.map(missingTab => Object.assign({}, getDefaultTabConfig(missingTab), { filter: missingTab, name: missingTab}));
        }
        tabConfig.tabs = tabConfig.tabs.concat(missingTabsFromConfig);

        this.setState({
          tabsList,
          tabConfig
        });
      });
  }

  handleTabClick(id) {
    let searchFilter = find(this.state.tabConfig.tabs, { id }).filter;
    MarketAction.setFilter(searchFilter);
  }

  componentWillUnmount() {
    MarketStore.dispatch({type: 'RESET'});
    this.marketStoreSubscription();
  }
  render() {
    return (
      <ConfigurableTab
        tabConfig={this.state.tabConfig}
        onTabClick={this.handleTabClick.bind(this)}
      />
    );
  }
}
