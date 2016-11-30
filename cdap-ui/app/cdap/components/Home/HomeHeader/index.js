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

require('./HomeHeader.less');

export default class HomeHeader extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isFilterExpanded: false,
      isSortExpanded: false
    };

    this.debouncedHandleSearch = debounce(this.handleSearch.bind(this), 500);
  }

  handleFilterToggle() {
    this.setState({isFilterExpanded: !this.state.isFilterExpanded});
  }

  handleSortToggle() {
    this.setState({isSortExpanded: !this.state.isSortExpanded});
  }

  handleSearch() {
    this.props.onSearch(this.searchBox.value);
  }

  render() {

    const placeholder = T.translate('features.Home.Header.search-placeholder');

    const sortDropdown = (
      <Dropdown
        isOpen={this.state.isSortExpanded}
        toggle={this.handleSortToggle.bind(this)}
      >
        <DropdownToggle tag='div'>
          {this.state.isSortExpanded ?
            <span>{T.translate('features.Home.Header.sort')}</span> :
            <span>{this.props.activeSort.displayName}</span>
          }
          <span className="fa fa-caret-down pull-right"></span>
        </DropdownToggle>
        <DropdownMenu>
          {
            this.props.sortOptions.map((option, index) => {
              return (
                <DropdownItem
                  key={index}
                  onClick={this.props.onSortClick.bind(this, option)}
                >
                  {option.displayName}
                  {
                    this.props.activeSort.fullSort === option.fullSort ?
                    <span className="fa fa-check pull-right"></span> :
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
          <span>{T.translate('features.Home.Header.filters')}</span>
          <span className="fa fa-filter pull-right"></span>
        </DropdownToggle>
        <DropdownMenu onClick={e => e.stopPropagation()}>
          {
            this.props.filterOptions.map((option) => {
              return (
                <DropdownItem
                  key={option.id}
                >
                  <div className="checkbox">
                    <label onClick={e => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={this.props.activeFilter.includes(option.id)}
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
      <div className="home-header">
        <div className="search-box">
          <div className="form-group has-feedback">
            <label className="control-label sr-only">
              {T.translate('features.Home.Header.search-placeholder')}
            </label>
            <input
              type="text"
              className="form-control"
              placeholder={placeholder}
              defaultValue={this.props.searchText}
              onChange={this.debouncedHandleSearch}
              ref={ref => this.searchBox = ref}
            />
            <span className="fa fa-search form-control-feedback"></span>
          </div>
        </div>
        <div className="filter">
          {filterDropdown}
        </div>
        <div className="view-selector pull-right">
          <div className="sort">
            {sortDropdown}
          </div>
          <div className="pagination-dropdown">
            <PaginationDropdown
              numberOfPages={this.props.numberOfPages}
              currentPage={this.props.currentPage}
              onPageChange={this.props.onPageChange}
            />
          </div>
          <span className="fa fa-th active"></span>
          <span className="fa fa-list"></span>
        </div>
      </div>
    );
  }
}

HomeHeader.propTypes = {
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
  onSortClick: PropTypes.func,
  onSearch: PropTypes.func,
  searchText: PropTypes.string,
  numberOfPages: PropTypes.number,
  currentPage: PropTypes.number,
  onPageChange: PropTypes.func
};
