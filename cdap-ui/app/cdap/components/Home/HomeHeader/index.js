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
require('./HomeHeader.less');
var classNames = require('classnames');

export default class HomeHeader extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isFilterExpanded: false,
      isSortExpanded: false
    };
  }

  handleFilterToggle() {
    this.setState({isFilterExpanded: !this.state.isFilterExpanded});
  }

  handlePreventPropagation(event) {
    event.stopPropagation();
  }

  render() {
    let filterDropdown;

    if (this.state.isFilterExpanded) {
      filterDropdown = (
        <div className="dropdown"
          onClick={this.handlePreventPropagation.bind(this)}
        >
          <ul className="list-unstyled">
            {
              this.props.filterOptions.map((option) => {
                return (
                  <li key={option.id}>
                    <div className="checkbox">
                      <label>
                        <input
                          type="checkbox"
                          checked={this.props.activeFilter.includes(option.id)}
                          onChange={this.props.onFilterClick.bind(this, option)}
                        />
                        {option.displayName}
                      </label>
                    </div>
                  </li>
                );
              })
            }
          </ul>
        </div>
      );
    }

    return (
      <div className="home-header">
        <div className="search-box">
          <div className="form-group has-feedback">
            <label className="control-label sr-only">Search Entity</label>
            <input
              type="text"
              className="form-control"
              placeholder="Search cards"
            />
            <span className="fa fa-search form-control-feedback"></span>
          </div>
        </div>

        <div className="sort">
          <span>Sort</span>
          <span className="fa fa-caret-down pull-right"></span>
        </div>

        <div className={classNames('filter', { 'active': this.state.isFilterExpanded })}>
          <div onClick={this.handleFilterToggle.bind(this)}>
            <span>Filters</span>
            <span className="fa fa-filter pull-right"></span>
          </div>

          {filterDropdown}

        </div>

        <div className="view-selector pull-right">
          <span className="fa fa-th active"></span>
          <span className="fa fa-list"></span>
        </div>

      </div>
    );
  }
}

HomeHeader.propTypes = {
  filterOptions: PropTypes.array,
  onFilterClick: PropTypes.func,
  activeFilter: PropTypes.array
};
