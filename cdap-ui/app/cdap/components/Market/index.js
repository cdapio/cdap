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

import React, { Component } from 'react';
import ConfigurableTab from '../ConfigurableTab';
import { MyMarketApi } from 'api/market';
import MarketAction from './action/market-action.js';
import find from 'lodash/find';
import MarketStore from 'components/Market/store/market-store.js';
import T from 'i18n-react';
import AllTabContents from 'components/Market/AllTab';
import UsecaseTab from 'components/Market/UsecaseTab';
import { CATEGORY_MAP, DEFAULT_CATEGORIES } from 'components/Market/CategoryMap';
import { objectQuery } from 'services/helpers';
import { Observable } from 'rxjs/Observable';

function hasMultipleMarkets() {
  return window.CDAP_CONFIG.marketUrls.length > 1;
}

export default class Market extends Component {
  constructor(props) {
    super(props);

    this.state = {
      tabConfig: null,
      activeTab: 1,
      marketsTabConfig: null,
      marketsActiveTab: 1,
      marketNames: [],
    };
  }

  componentDidMount() {
    this.sub = MarketStore.subscribe(() => {
      let activeFilter = MarketStore.getState().filter;
      let filter = find(this.state.tabConfig.tabs, { filter: activeFilter });

      if (filter && filter.id !== this.state.activeTab) {
        this.setState({
          activeTab: filter.id,
        });
      }
    });

    if (hasMultipleMarkets()) {
      this.populateMarketNames();
      return;
    }

    this.populateMarketPackages(window.CDAP_CONFIG.marketUrls[0]);
  }

  populateMarketPackages(marketHost) {
    MyMarketApi.list({ marketHost }).subscribe(
      (packages) => {
        this.getCategories(packages, marketHost);
      },
      () => {
        this.handleMarketConnectionError(marketHost);
      }
    );
  }

  populateMarketNames() {
    if (!hasMultipleMarkets()) {
      return;
    }
    const markets = window.CDAP_CONFIG.marketUrls;

    // Cache all market names from market metadata.
    const observables = markets.map((marketHost) =>
      MyMarketApi.getMetaData({ marketHost }).catch(() => {
        // Fallback to "Unknown" as the market name. If the user clicks the unknown
        // market, a connection failure UI would most likly appear.
        return Observable.of({ marketName: marketHost });
      })
    );
    Observable.forkJoin(observables).subscribe((marketMetas) => {
      const marketNames = marketMetas.map((marketMeta) => marketMeta.marketName);
      this.setState({ marketNames }, () => {
        this.populateMarketPackages(markets[0]);
      });
    });
  }

  handleMarketConnectionError(marketHost) {
    this.processPackagesAndCategories([], [], marketHost);
    MarketAction.setError();
  }

  componentWillUnmount() {
    MarketStore.dispatch({ type: 'RESET' });

    if (this.sub) {
      this.sub();
    }
  }

  getCategories = (packages, marketHost) => {
    const filteredPackages = packages.filter((packet) => {
      return (
        packet.categories.indexOf('datapack') === -1 &&
        packet.categories.indexOf('gcp') === -1 &&
        packet.categories.indexOf('usecase') === -1
      );
    });

    MyMarketApi.getCategories({ marketHost }).subscribe(
      (categories) => {
        categories = categories.filter(
          (category) => ['gcp', 'usecase', 'datapack'].indexOf(category.name) === -1
        );
        this.processPackagesAndCategories(filteredPackages, categories, marketHost);
      },
      () => {
        // If categories do not come from backend, revert back to get categories from existing packages
        const categoriesMap = {};
        filteredPackages.forEach((pack) => {
          pack.categories.forEach((category) => {
            categoriesMap[category] = true;
          });
        });

        let aggregateCategories = [];

        DEFAULT_CATEGORIES.forEach((cat) => {
          if (categoriesMap[cat]) {
            aggregateCategories.push(cat);
            delete categoriesMap[cat];
          }
        });

        const remainingCategories = Object.keys(categoriesMap);

        aggregateCategories = aggregateCategories.concat(remainingCategories).map((cat) => {
          return {
            name: cat,
            hasIcon: false,
          };
        });

        this.processPackagesAndCategories(filteredPackages, aggregateCategories);
      }
    );
  };

  processPackagesAndCategories(filteredPackages, categories, marketHost) {
    const newState = {
      tabConfig: this.constructTabConfig(categories, marketHost),
    };
    const searchFilter = find(newState.tabConfig.tabs, { filter: MarketStore.getState().filter });

    if (searchFilter) {
      newState.activeTab = searchFilter.id;
    }

    if (hasMultipleMarkets()) {
      newState.marketsTabConfig = this.constructMarketTabConfig(newState);
    }

    this.setState(newState);
    MarketAction.setList(filteredPackages);
  }

  constructMarketTabConfig(newState) {
    const tabConfig = {
      defaultTab: 1,
      layout: 'horizontal',
    };

    const tabs = this.state.marketNames.map((marketName, index) => {
      return {
        id: index + 1,
        filter: marketName,
        marketHost: window.CDAP_CONFIG.marketUrls[index],
        name: marketName,
        content: (
          <ConfigurableTab
            tabConfig={newState.tabConfig}
            onTabClick={this.handleTabClick.bind(this)}
            activeTab={newState.activeTab}
          />
        ),
      };
    });
    tabConfig.tabs = tabs;
    return tabConfig;
  }

  constructTabConfig(categories, marketHost) {
    const tabConfig = {
      defaultTab: 1,
      defaultTabContent: <AllTabContents />,
      layout: 'vertical',
    };

    const tabs = [
      {
        id: 1,
        filter: '*',
        icon: {
          type: 'font-icon',
          arguments: {
            data: 'icon-all',
          },
        },
        name: T.translate('features.Market.tabs.all'),
        content: <AllTabContents />,
      },
    ];

    categories.forEach((category) => {
      const categoryContent = CATEGORY_MAP[category.name] || {};
      const name = categoryContent.displayName || category.name;

      let icon;

      if (category.hasIcon) {
        icon = {
          type: 'link',
          arguments: {
            url: MyMarketApi.getCategoryIcon(category.name, marketHost),
          },
        };
      } else if (categoryContent.displayName) {
        icon = {
          type: 'font-icon',
          arguments: {
            data: categoryContent.icon,
          },
        };
      } else {
        const name = objectQuery(category, 'name');
        let charIcon = 'icon-info';

        if (name) {
          charIcon = `icon-${name[0].toUpperCase()}`;
        }

        icon = {
          type: 'font-icon',
          arguments: {
            data: charIcon,
          },
        };
      }

      const config = {
        id: category.name,
        filter: category.name,
        name,
        icon,
        content: category.name === 'usecase' ? <UsecaseTab /> : <AllTabContents />,
      };

      tabs.push(config);
    });

    tabConfig.tabs = tabs;
    return tabConfig;
  }

  handleTabClick(id) {
    let searchFilter = find(this.state.tabConfig.tabs, { id }).filter;

    this.setState({ activeTab: id });
    MarketAction.setFilter(searchFilter);
  }

  handleMarketTabClick(id) {
    const marketHost = find(this.state.marketsTabConfig.tabs, { id }).marketHost;
    this.populateMarketPackages(marketHost);

    // Reset states before rendering market UI.
    this.setState({ activeTab: 1, marketsActiveTab: id });
    MarketStore.dispatch({ type: 'RESET' });
    MarketAction.setSelectedMarketHost(marketHost);
  }

  render() {
    if (!this.state.tabConfig) {
      return null;
    }

    if (!hasMultipleMarkets()) {
      return (
        <ConfigurableTab
          tabConfig={this.state.tabConfig}
          onTabClick={this.handleTabClick.bind(this)}
          activeTab={this.state.activeTab}
        />
      );
    }

    if (!this.state.marketsTabConfig) {
      return null;
    }

    return (
      <ConfigurableTab
        tabConfig={this.state.marketsTabConfig}
        onTabClick={this.handleMarketTabClick.bind(this)}
        activeTab={this.state.marketsActiveTab}
      />
    );
  }
}
