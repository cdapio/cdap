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
import WranglerStore from 'components/Wrangler/Store/WranglerStore';
import WranglerActions from 'components/Wrangler/Store/WranglerActions';
import ChartPanel from 'components/Wrangler/WranglerRightPanel/ChartPanel';
import shortid from 'shortid';

require('./WranglerRightPanel.less');

export default class WranglerRightPanel extends Component {
  constructor() {
    super();

    let initState = WranglerStore.getState();
    let visualizationState = initState.visualization;

    this.state = Object.assign({}, visualizationState, {
      columns: initState.wrangler.headersList,
      graphTypeSelected: 'line'
    });

    this.addGraph = this.addGraph.bind(this);
  }

  componentWillMount() {
    WranglerStore.subscribe(() => {
      let state = WranglerStore.getState();
      this.setState(Object.assign({}, state.visualization, {
        columns: state.wrangler.headersList
      }));
    });
  }

  addGraph() {
    WranglerStore.dispatch({
      type: WranglerActions.addChart,
      payload: {
        chart: {
          id: shortid.generate(),
          type: this.state.graphTypeSelected,
          x: '##',
          y: []
        }
      }
    });
  }

  render() {
    return (
      <div className="wrangler-right-panel">
        <div className="graph-selector clearfix">
          <div className="graph-dropdown pull-left">
            <select
              className="form-control"
              value={this.state.graphTypeSelected}
              onChange={e => this.setState({graphTypeSelected: e.target.value})}
            >
              <option value="line">Line</option>
              <option value="area">Area</option>
              <option value="bar">Bar</option>
              <option value="scatter">Scatter</option>
            </select>
          </div>

          <div className="graph-add-button text-center pull-right">
            <span
              className="fa fa-plus-circle"
              onClick={this.addGraph}
            />
          </div>
        </div>

        <div className="graphs-list">
          {
            this.state.chartOrder.map((chartId) => {
              return (
                <ChartPanel
                  chart={this.state.charts[chartId]}
                  key={chartId}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}
