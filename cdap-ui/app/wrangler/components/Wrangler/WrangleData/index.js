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
import WrangleHistory from 'wrangler/components/Wrangler/WrangleHistory';
import classnames from 'classnames';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import Filter from 'wrangler/components/Wrangler/Filter';
import WranglerRightPanel from 'wrangler/components/Wrangler/WranglerRightPanel';
import WranglerTable from 'wrangler/components/Wrangler/WranglerTable';
import AddToHydrator from 'wrangler/components/Wrangler/AddToHydrator';
import NamespaceStore from 'services/NamespaceStore';
import Rx from 'rx';
import {MyArtifactApi} from 'api/artifact';
import find from 'lodash/find';

export default class WrangleData extends Component {
  constructor(props, context) {
    super(props, context);

    let wrangler = WranglerStore.getState().wrangler;

    let stateObj = Object.assign({}, wrangler, {
      loading: false,
      activeSelection: null,
      showVisualization: false,
    });

    this.state = stateObj;

    this.onSort = this.onSort.bind(this);
    this.onVisualizationDisplayClick = this.onVisualizationDisplayClick.bind(this);
    this.undo = this.undo.bind(this);
    this.generateLinks = this.generateLinks.bind(this);

    this.tableHeader = null;
    this.tableBody = null;

   this.sub = WranglerStore.subscribe(() => {
      let state = WranglerStore.getState().wrangler;
      this.setState(state);
    });

  }

  componentDidMount() {
    this.forceUpdate();

    setTimeout(() => {
      let container = document.getElementsByClassName('data-table');

      let height = container[0].clientHeight;
      let width = container[0].clientWidth;

      this.setState({height, width});
    });
  }

  componentWillUnmount() {
    this.sub();
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

  onVisualizationDisplayClick() {
    this.setState({showVisualization: !this.state.showVisualization});
  }

  undo() {
    WranglerStore.dispatch({ type: WranglerActions.undo });
  }

  forward() {
    WranglerStore.dispatch({ type: WranglerActions.redo });
  }

  generateLinks() {
    let wranglerPluginProperties = this.props.onHydratorApply();
    let namespace = NamespaceStore.getState().selectedNamespace;

    return Rx.Observable.create((observer) => {
      MyArtifactApi.list({namespace})
        .subscribe((res) => {
          let batchArtifact = find(res, { 'name': 'cdap-data-pipeline' });
          let realtimeArtifact = find(res, { 'name': 'cdap-data-streams' });
          let wranglerArtifact = find(res, { 'name': 'wrangler' });

          // Generate hydrator config as URL parameters
          let config = {
            config: {
              source: {},
              transforms: [{
                name: 'Wrangler',
                plugin: {
                  name: 'Wrangler',
                  label: 'Wrangler',
                  artifact: wranglerArtifact,
                  properties: wranglerPluginProperties
                }
              }],
              sinks:[],
              connections: []
            }
          };

          let realtimeConfig = Object.assign({}, config, {artifact: realtimeArtifact});
          let batchConfig = Object.assign({}, config, {artifact: batchArtifact});

          let realtimeUrl = window.getHydratorUrl({
            stateName: 'hydrator.create',
            stateParams: {
              namespace: namespace,
              configParams: realtimeConfig
            }
          });

          let batchUrl = window.getHydratorUrl({
            stateName: 'hydrator.create',
            stateParams: {
              namespace: namespace,
              configParams: batchConfig
            }
          });

          observer.onNext({realtimeUrl, batchUrl});
          observer.onCompleted();

        });
    });
  }

  renderHydratorButton () {
    const applyToHydrator = (
      <button
        className="btn btn-primary"
        onClick={this.props.onHydratorApply}
      >
        <span className="fa icon-hydrator" />
        Apply to Hydrator
      </button>
    );

    const jumpToHydrator = (
      <AddToHydrator linkGenerator={this.generateLinks} />
    );

    return this.context.source === 'hydrator' ? applyToHydrator : jumpToHydrator;
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

          <WrangleHistory
            historyArray={this.state.history.slice(0, this.state.historyLocation)}
          />

        </div>

        <div className={classnames('wrangle-results', {
          expanded: !this.state.showVisualization
        })}>
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

            <div className="pull-right action-button-container">

              <div className="hydrator-button">
                {this.renderHydratorButton()}
              </div>

              {
                !this.state.showVisualization ? (
                  <div
                    className="action-button text-center"
                    onClick={this.onVisualizationDisplayClick}
                  >
                    <span className="fa fa-bar-chart" />
                  </div>
                ) : null
              }
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
                  height={this.state.height}
                  width={this.state.width}
                />
              )
            }
          </div>
        </div>

        {
          this.state.showVisualization ? (
            <WranglerRightPanel toggle={this.onVisualizationDisplayClick} />
          ) : null
        }
      </div>
    );
  }
}

WrangleData.propTypes = {
  onHydratorApply: PropTypes.func
};

WrangleData.contextTypes = {
  source: PropTypes.oneOf(['wrangler', 'hydrator'])
};
