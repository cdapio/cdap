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
import WrangleHistory from 'components/Wrangler/WrangleHistory';
import classnames from 'classnames';
import shortid from 'shortid';
import Histogram from 'components/Wrangler/Histogram';
import WranglerStore from 'components/Wrangler/Redux/WranglerStore';
import WranglerActions from 'components/Wrangler/Redux/WranglerActions';
import ColumnActionsDropdown from 'components/Wrangler/ColumnActionsDropdown';
import orderBy from 'lodash/orderBy';
import Filter from 'components/Wrangler/Filter';

export default class WrangleData extends Component {
  constructor(props) {
    super(props);

    let wrangler = WranglerStore.getState().wrangler;

    let stateObj = Object.assign({}, wrangler, {
      loading: false,
      activeSelection: null,
      showHistogram: false,
    });

    this.state = stateObj;

    this.onSort = this.onSort.bind(this);
    this.onHistogramDisplayClick = this.onHistogramDisplayClick.bind(this);
    this.undo = this.undo.bind(this);

    WranglerStore.subscribe(() => {
      let state = WranglerStore.getState().wrangler;
      this.setState(state);
    });
  }

  onColumnClick(column) {
    this.setState({activeSelection: column});
  }

  onSort() {
    WranglerStore.dispatch({
      type: WranglerActions.sortColumn,
      payload: {
        activeColumn: this.state.activeSelection
      }
    });
  }

  onHistogramDisplayClick() {
    this.setState({showHistogram: !this.state.showHistogram});
  }

  renderHistogramRow() {
    if (!this.state.showHistogram) { return null; }

    const headers = this.state.headersList;

    return (
      <tr>
        <th className="index-column">
          <span className="fa fa-bar-chart"></span>
        </th>
        {
          headers.map((head) => {
            return (
              <th
                key={head}
                className={classnames({
                  active: this.state.activeSelection === head
                })}
              >
                <Histogram
                  data={this.state.histogram[head].data}
                  labels={this.state.histogram[head].labels}
                />
              </th>
            );
          })
        }
      </tr>
    );
  }

  undo() {
    WranglerStore.dispatch({ type: WranglerActions.undo });
  }

  forward() {
    WranglerStore.dispatch({ type: WranglerActions.redo });
  }

  filterData(data, column, filterBy, ignoreCase) {
    function _equal(row) {
      let columnData = row[column];
      let filterData = filterBy;

      if (ignoreCase) {
        columnData = columnData.toLowerCase();
        filterData = filterData.toLowerCase();
      }

      return columnData === filterData;
    }

    function _notEqual(row) {
      let columnData = row[column];
      let filterData = filterBy;

      if (ignoreCase) {
        columnData = columnData.toLowerCase();
        filterData = filterData.toLowerCase();
      }

      return columnData !== filterData;
    }

    function _lessThan(row) {
      return parseFloat(row[column]) < parseFloat(filterBy);
    }

    function _greaterThan(row) {
      return parseFloat(row[column]) > parseFloat(filterBy);
    }

    function _lessEqualThan(row) {
      return parseFloat(row[column]) <= parseFloat(filterBy);
    }

    function _greaterEqualThan(row) {
      return parseFloat(row[column]) >= parseFloat(filterBy);
    }

    function _startsWith(row) {
      let columnData = row[column];
      let filterData = filterBy;

      if (ignoreCase) {
        columnData = columnData.toLowerCase();
        filterData = filterData.toLowerCase();
      }

      return columnData.substr(0, filterData.length) === filterData;
    }

    function _endsWith(row) {
      let columnData = row[column];
      let filterData = filterBy;

      if (ignoreCase) {
        columnData = columnData.toLowerCase();
        filterData = filterData.toLowerCase();
      }

      let position = columnData.length - filterData.length;
      return columnData.substr(position) === filterData;
    }

    function _contains(row) {
      const ignoreCase = this.state.filterIgnoreCase;
      let columnData = row[column];
      let filterData = filterBy;

      if (ignoreCase) {
        columnData = columnData.toLowerCase();
        filterData = filterData.toLowerCase();
      }

      return columnData.indexOf(filterData) !== -1;
    }

    const functionsMap = {
      '=': _equal,
      '!=': _notEqual,
      '<': _lessThan,
      '>': _greaterThan,
      '<=': _lessEqualThan,
      '>=': _greaterEqualThan,
      'startsWith': _startsWith,
      'endsWith': _endsWith,
      'contains': _contains
    };
    const filterFunction = this.state.filter.filterFunction;

    return data.filter(functionsMap[filterFunction].bind(this));
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="loading text-center">
          <div>
            <span className="fa fa-spinner fa-spin"></span>
          </div>
          <h3>Wrangling...</h3>
        </div>
      );
    }

    const headers = this.state.headersList;
    const originalData = this.state.data;
    const errors = this.state.errors;

    let data = originalData;
    if (this.state.sort) {
      let sortOrder = this.state.sortAscending ? 'asc' : 'desc';
      data = orderBy(originalData, [this.state.sort], [sortOrder]);
    }

    if (this.state.filter) {
      const filter = this.state.filter;
      data = this.filterData(data, filter.column, filter.filterBy, filter.filterIgnoreCase);
    }

    const errorCount = headers.reduce((prev, curr) => {
      let count = errors[curr] ? errors[curr].count : 0;
      return prev + count;
    }, 0);

    const errorCircle = <i className="fa fa-circle error"></i>;

    return (
      <div className="wrangler-data row">
        <div className="wrangle-transforms">
          <div className="wrangle-filters text-center">
            <span
              className="fa fa-undo"
              onClick={this.undo}
            />
            <span
              className="fa fa-repeat"
              onClick={this.forward}
            />
            <span className="fa fa-filter"></span>
          </div>

          <div
            className={classnames('transform-item', { disabled: !this.state.activeSelection})}
            onClick={this.onSort}
          >
            <span className="fa fa-long-arrow-up" />
            <span className="fa fa-long-arrow-down" />
            <span className="transform-item-text">Sort</span>

            <span className="pull-right sort-indicator">
              <span className={classnames('fa', {
                'fa-long-arrow-down': this.state.sortAscending,
                'fa-long-arrow-up': !this.state.sortAscending
              })} />
            </span>
          </div>

          <Filter column={this.state.activeSelection} />

          <div
            className="transform-item"
            onClick={this.onHistogramDisplayClick}
          >
            <span className="fa fa-bar-chart"></span>
            <span className="transform-item-text">
              <span>{ this.state.showHistogram ? 'Hide' : 'Show'}</span>
              <span>Histogram</span>
            </span>

          </div>

          <WrangleHistory
            historyArray={this.state.history.slice(0, this.state.historyLocation)}
          />

        </div>

        <div className="wrangle-results">
          <div className="wrangler-data-metrics">
            <div className="metric-block">
              <h3 className="text-success">{this.state.data.length}</h3>
              <h5>Rows</h5>
            </div>

            <div className="metric-block">
              <h3 className="text-success">{this.state.headersList.length}</h3>
              <h5>Columns</h5>
            </div>

            <div className="metric-block">
              <h3 className="text-danger">{errorCount}</h3>
              <h5>Errors</h5>
            </div>
          </div>

          <div className="data-table">
            <table className="table table-bordered">
              <thead>
                <tr>
                  <th className="index-column text-center">#</th>
                  {
                    headers.map((head) => {
                      return (
                        <th
                          className={classnames('top-header', {
                            active: this.state.activeSelection === head
                          })}
                          key={head}
                        >
                          <span
                            className="header-text"
                            onClick={this.onColumnClick.bind(this, head)}
                          >
                            {head}
                          </span>
                          <span className="pull-right">
                            {errors[head] && errors[head].count ? errorCircle : null}
                            <ColumnActionsDropdown column={head} />
                          </span>
                        </th>
                      );
                    })
                  }
                </tr>
                <tr className="column-type-row">
                  <th className="index-column"></th>
                  {
                    headers.map((head) => {
                      return (
                        <th
                          className={classnames({
                            active: this.state.activeSelection === head
                          })}
                          key={head}
                        >
                          {this.state.columnTypes[head]}
                        </th>
                      );
                    })
                  }
                </tr>
                {this.renderHistogramRow()}
              </thead>

              <tbody>
                { data.map((row, index) => {
                  return (
                    <tr key={shortid.generate()}>
                      <td className="index-column text-center">
                        <span className="content">{index+1}</span>
                      </td>
                      {
                        headers.map((head) => {
                          return (
                            <td
                              key={shortid.generate()}
                              className={classnames({
                                active: this.state.activeSelection === head
                              })}
                            >
                              <span className="content">{row[head]}</span>
                              {errors[head] && errors[head][index] ? errorCircle : null}
                            </td>
                          );
                        })
                      }
                    </tr>
                  );
                }) }
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  }
}

WrangleData.defaultProps = {
  data: []
};

WrangleData.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object)
};
