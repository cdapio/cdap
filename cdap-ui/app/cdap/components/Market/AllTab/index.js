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
// import SearchTextBox from '../SearchTextBox';
import MarketPlaceEntity from 'components/MarketPlaceEntity';
import T from 'i18n-react';
import MarketStore from 'components/Market/store/market-store.js';
import Fuse from 'fuse.js';
require('./AllTabContents.less');
import classnames from 'classnames';

export default class AllTabContents extends Component {
  constructor(props) {
    super(props);
    this.state = {
      searchStr: '',
      entities: this.getFilteredEntities(),
      loading: MarketStore.getState().loading,
      isError: MarketStore.getState().isError
    };

    this.unsub = MarketStore.subscribe(() => {
      this.setState({entities: this.getFilteredEntities()});
      const {loading, isError} = MarketStore.getState();
      this.setState({loading, isError});
    });
  }

  componentWillUnmount () {
    this.unsub();
  }

  getFilteredEntities() {
    const {list, filter} = MarketStore.getState();
    if (filter === '*') {
      return list;
    }

    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: [
        "categories"
      ]
    };

    let fuse = new Fuse(list, fuseOptions);
    return fuse.search(filter);
  }

  onSearch(changeEvent) {
    // For now just save. Eventually we will make a backend call to get the search result.
    this.setState({searchStr: changeEvent.target.value});
  }


  handleBodyRender() {
    if (this.state.isError) { return null; }

    const loadingElem = (
      <h4>
        <span className="fa fa-spinner fa-spin fa-2x"></span>
      </h4>
    );
    const empty = <h3>{T.translate('features.Market.tabs.emptyTab')}</h3>;
    const entities = (
      this.state.entities
        .map((e) => (
          <MarketPlaceEntity
            key={e.id}
            entityId={e.id}
            entity={e}
          />
        )
      )
    );

    if (this.state.loading) {
      return loadingElem;
    } else if (this.state.entities.length === 0) {
      return empty;
    } else {
      return entities;
    }
  }

  render() {
    let error;
    if (this.state.isError) {
      error = (
        <h3 className="error-message">
          {T.translate('features.Market.connectErrorMessage')}
        </h3>
      );
    }

    return (
      <div className="all-tab-content">
        {/*
          <SearchTextBox
            placeholder={T.translate('features.Market.search-placeholder')}
            value={this.state.searchStr}
            onChange={this.onSearch.bind(this)}
          />
        */}
        <div className={classnames("body-section text-center", {'empty-section': this.state.entities.length === 0 })}>
          {error}
          {this.handleBodyRender()}
        </div>
      </div>
    );
  }
}
