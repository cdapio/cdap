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
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import Charts from 'wrangler/components/Wrangler/Charts';
import xor from 'lodash/xor';
import classnames from 'classnames';

export default class ChartPanel extends Component {
  constructor(props) {
    super(props);

    const headersList = WranglerStore.getState().wrangler.headersList;

    this.state = {
      columns: headersList,
      isExpanded: true,
      yValue: headersList[0]
    };

    this.onExpandClick = this.onExpandClick.bind(this);
    this.addYValue = this.addYValue.bind(this);
  }

  componentWillMount() {
    WranglerStore.subscribe(() => {
      this.setState({columns: WranglerStore.getState().wrangler.headersList});
    });
  }

  onExpandClick() {
    this.setState({isExpanded: !this.state.isExpanded});
  }

  onXChange(event, graph) {
    graph.x = event.target.value;

    WranglerStore.dispatch({
      type: WranglerActions.editChart,
      payload: {
        chart: graph
      }
    });
  }

  removeYValue(value) {
    let newGraphSpec = Object.assign({}, this.props.chart);

    newGraphSpec.y.splice(newGraphSpec.y.indexOf(value), 1);

    WranglerStore.dispatch({
      type: WranglerActions.editChart,
      payload: {
        chart: newGraphSpec
      }
    });
  }

  addYValue() {
    const columnToAdd = this.state.yValue;
    if (!columnToAdd) { return; }

    let newGraphSpec = Object.assign({}, this.props.chart);
    newGraphSpec.y.push(columnToAdd);

    let availableYOptions = xor(this.state.columns, newGraphSpec.y);

    this.setState({
      yValue: availableYOptions.length ? availableYOptions[0] : ''
    });

    WranglerStore.dispatch({
      type: WranglerActions.editChart,
      payload: {
        chart: newGraphSpec
      }
    });
  }

  renderPanelContent() {
    if (!this.state.isExpanded) { return null; }

    const graph = this.props.chart;

    let chart;
    if (graph.x && graph.y.length > 0) {
      chart = <Charts spec={graph} />;
    }

    let yOptions = xor(this.state.columns, graph.y);

    let ySelect;
    if (yOptions.length) {
      ySelect = (
        <div className="y-axis-selector">
          <div className="y-value-selector">
            <select
              className="form-control"
              value={this.state.yValue}
              onChange={e => this.setState({yValue: e.target.value})}
            >
              {yOptions.map((column) => {
                return (
                  <option
                    key={column}
                    value={column}
                  >
                    {column}
                  </option>
                );
              })}
            </select>
          </div>

          <div className="add-y-value-button">
            <div className="y-axis-add-button text-center">
              <span
                className="fa fa-plus-circle"
                onClick={this.addYValue}
              />
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="graph-content">
        <div className="graph-diagram">
          {chart}
        </div>

        <div className="x-axis-selector">
          <h5>X Axis</h5>
          <select
            className="form-control"
            value={graph.x}
            onChange={e => this.onXChange(e, graph)}
          >
            <option value="##">##</option>
            {this.state.columns.map((column) => {
              return (
                <option
                  key={column}
                  value={column}
                >
                  {column}
                </option>
              );
            })}
          </select>
        </div>

        <div className="y-axis-selector">
          <h5>Y Axis</h5>

          <div className="y-values-list">
            <ul>
              {
                graph.y.map((value) => {
                  return (
                    <li key={value}>
                      <span>{value}</span>
                      <span
                        className="fa fa-times-circle pull-right"
                        onClick={this.removeYValue.bind(this, value)}
                      />
                    </li>
                  );
                })
              }
            </ul>
          </div>
          {ySelect}
        </div>
      </div>
    );
  }

  render() {
    const graph = this.props.chart;

    return (
      <div
        className="graph-row"
        key={graph.id}
      >
        <div
          className="graph-header"
          onClick={this.onExpandClick}
        >
          <h5>
            <span>{graph.label}</span>
            <span
              className={classnames('fa pull-right', {
                'fa-chevron-up': this.state.isExpanded,
                'fa-chevron-down': !this.state.isExpanded
              })}
            />
          </h5>
        </div>

        {this.renderPanelContent()}
      </div>
    );
  }
}

ChartPanel.propTypes = {
  chart: PropTypes.object
};
