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
import PaginationDropdown from 'components/Pagination/PaginationDropdown';
import {Tooltip} from 'reactstrap';

require('./EntityListHeader.less');

export default class EntityListHeader extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isFilterExpanded: false,
      isSortExpanded: false,
      searchText: props.searchText,
      sortOptions: props.sortOptions,
      filterOptions: props.filterOptions,
      numberOfPages: props.numberOfPages,
      currentPage: props.currentPage,
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
      numberOfPages: nextProps.numberOfPages,
      currentPage: nextProps.currentPage,
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
          <DropdownToggle tag='div'>
            {this.state.isSortExpanded ?
              <span>{T.translate('features.EntityListView.Header.sort')}</span> :
              <span>{T.translate('features.EntityListView.Header.sortLabel')}{this.state.activeSort.displayName}</span>
            }
            <span className="fa fa-caret-down pull-right"></span>
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
                        <span className="fa fa-check pull-right"></span>
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
        <DropdownToggle tag='div'>
          <span>{T.translate('features.EntityListView.Header.filters')}</span>
          <span className="fa fa-filter pull-right"></span>
        </DropdownToggle>
        <DropdownMenu onClick={e => e.stopPropagation()}>
          {
            this.state.filterOptions.map((option) => {
              return (
                <DropdownItem
                  key={option.id}
                >
                  <div className="checkbox">
                    <label onClick={e => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={this.state.activeFilter.indexOf(option.id) !== -1}
                        onChange={this.props.onFilterClick.bind(this, option)}
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
      <div className="entity-list-header">
        <div className="search-box">
          <div className="form-group has-feedback">
            <label className="control-label sr-only">
              {T.translate('features.EntityListView.Header.search-placeholder')}
            </label>
            <input
              disabled={this.props.isSearchDisabled ? 'disabled': null}
              type="text"
              className="form-control"
              placeholder={placeholder}
              value={this.state.searchText}
              onChange={this.onSearchChange.bind(this)}
            />
            <span className="fa fa-search form-control-feedback"></span>
          </div>
        </div>
        <div className="filter">
          {filterDropdown}
          <Tooltip
            placement="top"
            isOpen={this.state.sortDropdownTooltip}
            target={tooltipId}
            toggle={this.toggleSortDropdownTooltip.bind(this)}
            delay={0}
          >
            {T.translate(`features.EntityListView.Header.sortdropdown-tooltip`)}
          </Tooltip>
        </div>
        <div className="view-selector pull-right">
          <div className="sort">
            {sortDropdown}
          </div>
          <div className="pagination-dropdown">
            <PaginationDropdown
              numberOfPages={this.state.numberOfPages}
              currentPage={this.state.currentPage}
              onPageChange={this.props.onPageChange}
            />
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
  numberOfPages: PropTypes.number,
  currentPage: PropTypes.number,
  onPageChange: PropTypes.func
};
