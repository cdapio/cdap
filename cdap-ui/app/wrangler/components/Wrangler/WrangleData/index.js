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
import WrangleHistory from 'wrangler/components/Wrangler/WrangleHistory';
import classnames from 'classnames';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import Filter from 'wrangler/components/Wrangler/Filter';
import WranglerRightPanel from 'wrangler/components/Wrangler/WranglerRightPanel';

import WranglerTable from 'wrangler/components/Wrangler/WranglerTable';

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

    this.tableHeader = null;
    this.tableBody = null;

    WranglerStore.subscribe(() => {
      let state = WranglerStore.getState().wrangler;
      this.setState(state);
    });
  }

  componentDidMount() {
    this.forceUpdate();

    let container = document.getElementsByClassName('data-table');

    let height = container[0].clientHeight;
    let width = container[0].clientWidth;

    this.setState({height, width});
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

  undo() {
    WranglerStore.dispatch({ type: WranglerActions.undo });
  }

  forward() {
    WranglerStore.dispatch({ type: WranglerActions.redo });
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
    const errors = this.state.errors;

    const errorCount = headers.reduce((prev, curr) => {
      let count = errors[curr] ? errors[curr].count : 0;
      return prev + count;
    }, 0);


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

          <div
            className="data-table"
          >
            {
              !this.state.height || !this.state.width ? null : (
                <WranglerTable
                  onColumnClick={this.onColumnClick.bind(this)}
                  activeSelection={this.state.activeSelection}
                  showHistogram={this.state.showHistogram}
                  height={this.state.height}
                  width={this.state.width}
                />
              )
            }
          </div>
        </div>

        <WranglerRightPanel />
      </div>
    );
  }
}
