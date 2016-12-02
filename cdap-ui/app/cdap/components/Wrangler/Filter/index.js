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

import React, { Component, PropTypes } from 'react';
import WranglerStore from 'components/Wrangler/Store/WranglerStore';
import WranglerActions from 'components/Wrangler/Store/WranglerActions';
import classnames from 'classnames';

export default class Filter extends Component {
  constructor(props) {
    super(props);

    this.state = {
      showFilter: false,
      filterIgnoreCase: false,
      filterFunction: '='
    };

    this.onFilterClick = this.onFilterClick.bind(this);
    this.onFilter = this.onFilter.bind(this);
  }

  onFilterClick() {
    if (!this.props.column) { return; }
    this.setState({showFilter: !this.state.showFilter});
  }

  renderFilter() {
    if (!this.state.showFilter || !this.props.column) { return null; }

    return (
      <div
        className="filter-input"
        onClick={e => e.stopPropagation()}
      >
        <select
          className="form-control"
          onChange={e => this.setState({filterFunction: e.target.value})}
          value={this.state.filterFunction}
        >
          <option value="=">Equal</option>
          <option value="!=">Does not equal</option>
          <option value="<">Less than</option>
          <option value=">">Greater than</option>
          <option value="<=">Less or equal to</option>
          <option value=">=">Greater or equal to</option>
          <option value="startsWith">Starts With</option>
          <option value="endsWith">Ends With</option>
          <option value="contains">Contains</option>
        </select>

        <div>
          <label className="label-control">Filter by</label>
          <input
            type="text"
            className="form-control"
            onChange={(e) => this.filterByText = e.target.value}
          />
        </div>

        <div className="checkbox">
          <label className="control-label">
            <input
              type="checkbox"
              checked={this.state.filterIgnoreCase}
              onChange={() => this.setState({filterIgnoreCase: !this.state.filterIgnoreCase})}
            />
            Ignore Case
          </label>
        </div>
        <br/>
        <div className="text-right">
          <button
            className="btn btn-primary"
            onClick={this.onFilter}
          >
            Apply
          </button>
        </div>
      </div>
    );
  }

  onFilter() {
    if (!this.filterByText) {
      this.setState({filter: null});
      WranglerStore.dispatch({
        type: WranglerActions.setFilter,
        payload: {
          filter: null
        }
      });
    } else {
      let filterObj = {
        column: this.props.column,
        filterBy: this.filterByText,
        filterFunction: this.state.filterFunction,
        filterIgnoreCase: this.state.filterIgnoreCase
      };

      this.setState({
        filter: filterObj
      });

      WranglerStore.dispatch({
        type: WranglerActions.setFilter,
        payload: {
          filter: filterObj
        }
      });
    }
  }

  render() {
    return (
      <div
        className={classnames('transform-item', {
          disabled: !this.props.column,
          expanded: this.state.showFilter
        })}
        onClick={this.onFilterClick}
      >
        <span className="fa fa-font"></span>
        <span className="transform-item-text">Filter</span>

        {this.renderFilter()}
      </div>
    );
  }
}

Filter.propTypes = {
  column: PropTypes.string
};
