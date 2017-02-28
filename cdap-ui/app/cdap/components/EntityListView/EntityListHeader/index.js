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
import { Dropdown, DropdownToggle, DropdownItem } from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import T from 'i18n-react';
import debounce from 'lodash/debounce';
import ResourceCenterButton from 'components/ResourceCenterButton';
import {isDescendant} from 'services/helpers';
import Rx from 'rx';

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

  componentWillUnmount() {
    if (this.documentClick$ && this.documentClick$.dispose) {
      this.documentClick$.dispose();
    }
  }

  handleFilterToggle() {
    let newState = !this.state.isFilterExpanded;

    this.setState({isFilterExpanded: newState});

    if (newState) {
      let element = document.getElementById('app-container');

      this.documentClick$ = Rx.Observable.fromEvent(element, 'click')
        .subscribe((e) => {
          if (isDescendant(this.dropdownButtonRef, e.target) || !this.state.isFilterExpanded) {
            return;
          }

          this.handleFilterToggle();
        });
    } else {
      this.documentClick$.dispose();
    }
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
  onFilterClick(option, event) {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();

    if (this.props.onFilterClick) {
      this.props.onFilterClick(option);
    }
  }
  render() {
    let tooltipId = 'filter-tooltip-target-id';
    const placeholder = T.translate('features.EntityListView.Header.search-placeholder');
    const sortDropdown = (
      <Dropdown
        isOpen={this.state.isSortExpanded}
        toggle={this.handleSortToggle.bind(this)}
        id={tooltipId}
      >
        <DropdownToggle
          tag='div'
          className="sort-toggle"
        >
          <span>{this.state.activeSort.displayName}</span>
          <span className="fa fa-angle-down float-xs-right"></span>
        </DropdownToggle>
        <CustomDropdownMenu>
          {
            this.state.sortOptions.slice(1).map((option, index) => {
              return (
                <DropdownItem
                  tag="li"
                  key={index}
                  className="clearfix"
                  onClick={this.props.onSortClick.bind(this, option)}
                >
                  <span className="float-xs-left">{option.displayName}</span>
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
        </CustomDropdownMenu>
      </Dropdown>
    );

    const filterDropdown = (
      <Dropdown
        isOpen={this.state.isFilterExpanded}
        toggle={() => {}}
        onClick={this.handleFilterToggle.bind(this)}
      >
        <DropdownToggle
          tag='div'
          className="filter-toggle"
        >
          <span>{T.translate('features.EntityListView.Header.filterBy')}</span>
          <span className="fa fa-angle-down float-xs-right"></span>
        </DropdownToggle>
        <CustomDropdownMenu onClick={e => e.stopPropagation()}>
          {
            this.state.filterOptions.map((option) => {
              return (
                <DropdownItem
                  tag="li"
                  key={option.id}
                  onClick={this.onFilterClick.bind(this, option)}
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
        </CustomDropdownMenu>
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
          <div className="filter" ref={(ref) => this.dropdownButtonRef = ref}>
            {filterDropdown}
          </div>
          <div className="sort">
            <span className="sort-label">
              {T.translate('features.EntityListView.Header.sortLabel')}
            </span>
            {sortDropdown}
          </div>
        </div>
        <ResourceCenterButton />
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
  searchText: PropTypes.string
};
