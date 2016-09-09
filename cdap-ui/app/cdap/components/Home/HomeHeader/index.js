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
require('./HomeHeader.less');

export default class HomeHeader extends Component {
  constructor(props) {
    super(props);
  }
  render() {
    return (
      <div className='home-header'>
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

        <div className="filter">
          <span>Filters</span>
          <span className="fa fa-filter pull-right"></span>
        </div>

        <div className="view-selector pull-right">
          <span className="fa fa-th active"></span>
          <span className="fa fa-list"></span>
        </div>

      </div>
    );
  }
}
