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
import MarketPlaceEntity from 'components/MarketPlaceEntity';
import T from 'i18n-react';
import MarketStore from 'components/Market/store/market-store.js';
import Fuse from 'fuse.js';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';

require('./AllTabContents.scss');

export default class AllTabContents extends Component {
  constructor(props) {
    super(props);
    const filteredEntities = this.getFilteredEntities();
    this.state = {
      searchStr: '',
      entities: filteredEntities,
      filterEntites: filteredEntities,
      loading: MarketStore.getState().loading,
      isError: MarketStore.getState().isError,
    };

    this.unsub = MarketStore.subscribe(() => {
      const unSubFilteredEntities = this.getFilteredEntities();
      this.setState({
        entities: unSubFilteredEntities,
        filterEntites: unSubFilteredEntities,
        searchStr: '',
      });
      const { loading, isError } = MarketStore.getState();
      this.setState({ loading, isError });
    });
  }

  componentWillUnmount() {
    this.unsub();
  }

  getFilteredEntities() {
    const { list, filter } = MarketStore.getState();
    if (filter === '*') {
      return list;
    }

    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: ['categories'],
    };

    let fuse = new Fuse(list, fuseOptions);
    return fuse.search(filter);
  }

  onSearch(changeEvent) {
    let searchStr = changeEvent.target.value;
    //  it is a ui end filter, it only rerenders the plugins which name contains the string present in search bar.
    var results = this.state.entities;
    if (searchStr != '') {
      results = this.state.entities.filter(
        (value) => value.label.toLowerCase().indexOf(searchStr.toLowerCase()) >= 0
      );
    }
    this.setState({ searchStr: changeEvent.target.value, filterEntites: results });
  }

  handleBodyRender() {
    if (this.state.isError) {
      return null;
    }

    const loadingElem = (
      <h4>
        <span className="fa fa-spinner fa-spin fa-2x" />
      </h4>
    );
    const empty = <h2>{T.translate('features.Market.tabs.emptyTab')}</h2>;
    const entities = this.state.filterEntites.map((e) => (
      <MarketPlaceEntity key={e.id} entityId={e.id} entity={e} />
    ));

    if (this.state.loading) {
      return loadingElem;
    } else if (entities && entities.length === 0) {
      return empty;
    } else {
      return entities;
    }
  }

  render() {
    let error;
    if (this.state.isError) {
      error = (
        <h3 className="error-message">{T.translate('features.Market.connectErrorMessage')}</h3>
      );
    }

    return (
      <div className="all-tab-content">
        <div className="search-box input-group">
          <div className="input-feedback input-group-prepend">
            <div className="input-group-text">
              <IconSVG name="icon-search" />
            </div>
          </div>
          <input
            autoFocus
            type="text"
            className="search-input form-control"
            placeholder={T.translate('features.Market.search-placeholder')}
            value={this.state.searchStr}
            onChange={this.onSearch.bind(this)}
          />
        </div>

        <div
          className={classnames('body-section text-center', {
            'empty-section': this.state.filterEntites.length === 0,
          })}
        >
          {error}
          {this.handleBodyRender()}
        </div>
      </div>
    );
  }
}
