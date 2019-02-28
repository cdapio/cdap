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
import classnames from 'classnames';
import { MySearchApi } from 'api/search';
import NamespaceStore from 'services/NamespaceStore';
import { parseMetadata } from 'services/metadata-parser';
import { Dropdown, DropdownMenu, DropdownItem } from 'reactstrap';
import debounce from 'lodash/debounce';
import SpotlightModal from 'components/SpotlightSearch/SpotlightModal';
import Mousetrap from 'mousetrap';
import T from 'i18n-react';
import uuidV4 from 'uuid/v4';

require('./SpotlightSearch.scss');

const keyMap = {
  enter: 13,
  esc: 27,
  arrowUp: 38,
  arrowDown: 40,
};

const VIEW_RESULT_LIMIT = 5;

export default class SpotlightSearch extends Component {
  constructor(props) {
    super(props);

    this.handleSearchClick = this.handleSearchClick.bind(this);
    this.debounceSearch = debounce(this.handleSearch.bind(this), 300);
    this.handleCloseSearch = this.handleCloseSearch.bind(this);
    this.handleToggleModal = this.handleToggleModal.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);

    this.state = {
      showSearch: false,
      showResult: false,
      showModal: false,
      searchQuery: '',
      searchResults: {},
      focusIndex: 0,
    };
  }

  componentWillMount() {
    Mousetrap.bind('?', this.handleSearchClick.bind(this));
  }

  componentDidUpdate() {
    if (!this.state.showSearch) {
      return;
    }
    this.spotlightSearch.focus();
  }

  handleSearchClick(event) {
    if (event) {
      event.preventDefault();
    }

    this.setState({ showSearch: true });
    let app = document.getElementById('app-container');
    app.addEventListener('click', this.handleCloseSearch);
    document.body.addEventListener('keyup', this.handleCloseSearch);
  }

  handleCloseSearch(event) {
    // Proceed if and only if:
    //  1. Event object is null
    //  2. Escape key was pressed
    //  3. Target of the event is not the spotlightSearch
    if (event && event.keyCode !== 27 && event.target === this.spotlightSearch) {
      return;
    }
    let app = document.getElementById('app-container');
    document.body.removeEventListener('keyup', this.handleCloseSearch);
    app.removeEventListener('click', this.handleCloseSearch);

    this.setState({ showSearch: false, showResult: false });
  }

  handleResultToggle() {
    this.setState({
      showResult: false,
      showSearch: false,
    });
  }

  handleSearch() {
    if (!this.spotlightSearch) {
      return;
    }

    let query = this.spotlightSearch.value;
    if (query.length === 0) {
      return;
    }

    MySearchApi.search({
      namespace: NamespaceStore.getState().selectedNamespace,
      query: query + '*',
      limit: VIEW_RESULT_LIMIT,
      responseFormat: 'v6',
    }).subscribe((res) => {
      this.setState({
        searchResults: res,
        showResult: true,
        searchQuery: query,
        focusIndex: 0,
      });
    });
  }

  handleRenderResult() {
    if (this.state.searchQuery.length === 0) {
      return null;
    }

    let dropdown = (
      <Dropdown
        isOpen={this.state.showResult}
        toggle={this.handleResultToggle.bind(this)}
        className="search-results-dropdown"
      >
        <DropdownMenu>
          {this.state.searchResults.results
            .filter((entity, index) => index < VIEW_RESULT_LIMIT)
            .map(parseMetadata)
            .map((entity, index) => {
              return (
                <DropdownItem
                  key={uuidV4()}
                  tag="a"
                  className={classnames({ hover: this.state.focusIndex === index })}
                >
                  <span className="icon">
                    <span className={entity.icon} />
                  </span>
                  {entity.id}
                </DropdownItem>
              );
            })}
          {this.state.searchResults.total === 0 ? (
            <DropdownItem tag="a" disabled>
              {T.translate('features.SpotlightSearch.noResult')}
            </DropdownItem>
          ) : null}
          {this.state.searchResults.total <= VIEW_RESULT_LIMIT ? null : (
            <DropdownItem
              tag="a"
              onClick={this.handleToggleModal}
              className={classnames('text-center', {
                hover: this.state.focusIndex === VIEW_RESULT_LIMIT,
              })}
            >
              {T.translate('features.SpotlightSearch.showAll', {
                num: this.state.searchResults.total,
              })}
            </DropdownItem>
          )}
        </DropdownMenu>
      </Dropdown>
    );

    return dropdown;
  }

  handleKeyPress(event) {
    // preventDefault() is to prevent the cursor to jump to the beginning
    // when arrowUp is pressed
    if (event.keyCode === keyMap.arrowUp) {
      event.preventDefault();
      let focusIndex = this.state.focusIndex;
      focusIndex = focusIndex === 0 ? focusIndex : focusIndex - 1;
      this.setState({ focusIndex: focusIndex });
    } else if (event.keyCode === keyMap.arrowDown) {
      event.preventDefault();
      let focusIndex = this.state.focusIndex;

      let limit =
        this.state.searchResults.total > VIEW_RESULT_LIMIT
          ? VIEW_RESULT_LIMIT
          : this.state.searchResults.total - 1;
      focusIndex = focusIndex < limit ? focusIndex + 1 : focusIndex;
      this.setState({ focusIndex: focusIndex });
    } else if (event.keyCode === keyMap.enter) {
      // Check focus index, and go to entity.
      // If focusIndex is 5, then show modal
      if (this.state.focusIndex === VIEW_RESULT_LIMIT) {
        this.setState({ showModal: true, showSearch: false, showResult: false });
      }
    }
  }

  handleToggleModal() {
    this.setState({ showModal: !this.state.showModal });
  }

  handleRenderModal() {
    if (!this.state.showModal) {
      return null;
    }

    return (
      <SpotlightModal
        query={this.state.searchQuery}
        isOpen={this.state.showModal}
        toggle={this.handleToggleModal}
      />
    );
  }

  render() {
    let spotlightSearch;

    if (!this.state.showSearch) {
      spotlightSearch = <span className="fa fa-search not-open" onClick={this.handleSearchClick} />;
    } else {
      spotlightSearch = (
        <div className="form-group input-group">
          <input
            type="text"
            className="form-control"
            ref={(ref) => (this.spotlightSearch = ref)}
            onChange={this.debounceSearch}
            onKeyDown={this.handleKeyPress}
          />

          <span className="input-feedback">
            <span className="fa fa-search" />
          </span>
        </div>
      );
    }

    return (
      <div className={classnames('spotlight-search-container', { open: this.state.showSearch })}>
        {spotlightSearch}
        {this.handleRenderResult()}
        {this.handleRenderModal()}
      </div>
    );
  }
}
