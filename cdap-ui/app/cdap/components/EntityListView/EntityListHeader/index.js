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
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import T from 'i18n-react';
import debounce from 'lodash/debounce';

require('./EntityListHeader.scss');

export default class EntityListHeader extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isFilterExpanded: false,
      isSortExpanded: false,
      searchText: props.searchText,
      sortOptions: props.sortOptions,
      filterOptions: props.filterOptions,
      activeFilter: props.activeFilter,
      activeSort: props.activeSort
    };

    this.debouncedHandleSearch = debounce(this.handleSearch.bind(this), 500);
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      searchText: nextProps.searchText,
      sortOptions: nextProps.sortOptions,
      filterOptions: nextProps.filterOptions,
      activeFilter: nextProps.activeFilter,
      activeSort: nextProps.activeSort
    });
  }

  handleFilterToggle() {
    this.setState({isFilterExpanded: !this.state.isFilterExpanded});
  }

  handleSortToggle() {
    if (this.props.isSortDisabled) {
      return;
    }
    this.setState({isSortExpanded: !this.state.isSortExpanded});
  }

  onSearchChange(e) {
    this.setState({
      searchText: e.target.value
    });
    this.debouncedHandleSearch();
  }

  handleSearch() {
    this.props.onSearch(this.state.searchText);
  }

  toggleSortDropdownTooltip() {
    if (this.props.isSortDisabled) {
      this.setState({sortDropdownTooltip: !this.state.sortDropdownTooltip});
    }
  }
  onFilterClick(option) {
    if (this.props.onFilterClick) {
      this.props.onFilterClick(option);
    }
    this.setState({
      isFilterExpanded: false
    });
  }
  render() {
    let tooltipId = 'filter-tooltip-target-id';
    const placeholder = this.props.isSearchDisabled ?
      T.translate('features.EntityListView.Header.search-disabled-placeholder')
    :
      T.translate('features.EntityListView.Header.search-placeholder')
    ;
    const sortDropdown = (
      <Dropdown
        disabled={this.props.isSortDisabled}
        isOpen={this.state.isSortExpanded}
        toggle={this.handleSortToggle.bind(this)}
        id={tooltipId}
      >
        <DropdownToggle
          tag='div'
          className="sort-toggle"
        >
          {
            this.state.activeSort ?
              <span>{this.state.activeSort.displayName}</span>
            :
              'Relevance'
          }
          <span className="fa fa-angle-down float-xs-right"></span>
        </DropdownToggle>
        <DropdownMenu>
          {
            this.state.sortOptions.map((option, index) => {
              return (
                <DropdownItem
                  key={index}
                  onClick={this.props.onSortClick.bind(this, option)}
                >
                  {option.displayName}
                  {
                    this.state.activeSort.fullSort === option.fullSort ?
                      <span className="fa fa-check float-xs-right"></span>
                    :
                      null
                  }
                </DropdownItem>
              );
            })
          }
        </DropdownMenu>
      </Dropdown>
    );

    const filterDropdown = (
      <Dropdown
        isOpen={this.state.isFilterExpanded}
        toggle={this.handleFilterToggle.bind(this)}
      >
        <DropdownToggle
          tag='div'
          className="filter-toggle"
        >
          <span>{T.translate('features.EntityListView.Header.filterBy')}</span>
          <span className="fa fa-angle-down float-xs-right"></span>
        </DropdownToggle>
        <DropdownMenu onClick={e => e.stopPropagation()}>
          {
            this.state.filterOptions.map((option) => {
              return (
                <DropdownItem
                  key={option.id}
                >
                  <div className="form-check">
                    <label
                      className="form-check-label"
                      onClick={e => e.stopPropagation()}
                    >
                      <input
                        type="checkbox"
                        className="form-check-input"
                        checked={this.state.activeFilter.indexOf(option.id) !== -1}
                        onChange={this.onFilterClick.bind(this, option)}
                      />
                      {option.displayName}
                    </label>
                  </div>
                </DropdownItem>
              );
            })
          }
        </DropdownMenu>
      </Dropdown>
    );

    return (
      <div>
        <div className="entity-list-header">
          <div className="search-box input-group">
            <span className="input-feedback input-group-addon">
              <span className="fa fa-search"></span>
            </span>
            <input
              type="text"
              className="search-input form-control"
              placeholder={placeholder}
              value={this.state.searchText}
              onChange={this.onSearchChange.bind(this)}
            />
          </div>
          <div className="filter">
            {filterDropdown}
          </div>
          <div className="sort">
            <span className="sort-label">
              {T.translate('features.EntityListView.Header.sortLabel')}
            </span>
            {sortDropdown}
          </div>
        </div>
      </div>
    );
  }
}

EntityListHeader.propTypes = {
  filterOptions: PropTypes.arrayOf(
    PropTypes.shape({
      displayName : PropTypes.string,
      id: PropTypes.string
    })
  ),
  onFilterClick: PropTypes.func,
  activeFilter: PropTypes.arrayOf(PropTypes.string),
  sortOptions: PropTypes.arrayOf(
    PropTypes.shape({
      displayName : PropTypes.string,
      sort: PropTypes.string,
      order: PropTypes.string,
      fullSort: PropTypes.string
    })
  ),
  activeSort: PropTypes.shape({
    displayName : PropTypes.string,
    sort: PropTypes.string,
    order: PropTypes.string,
    fullSort: PropTypes.string
  }),
  isSortDisabled: PropTypes.bool,
  onSortClick: PropTypes.func,
  onSearch: PropTypes.func,
  isSearchDisabled: PropTypes.bool,
  searchText: PropTypes.string,
};
