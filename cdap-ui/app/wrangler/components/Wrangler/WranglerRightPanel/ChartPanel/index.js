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
      showAllX: false,
      showAllY: false
    };

    this.onExpandClick = this.onExpandClick.bind(this);
    this.addYValue = this.addYValue.bind(this);
    this.toggleShowAllX = this.toggleShowAllX.bind(this);
    this.toggleShowAllY = this.toggleShowAllY.bind(this);
  }

  componentWillMount() {
    this.sub = WranglerStore.subscribe(() => {
      this.setState({columns: WranglerStore.getState().wrangler.headersList});
    });
  }

  componentWillUnmount() {
    this.sub();
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

  removeYValue(column) {
    let newGraphSpec = Object.assign({}, this.props.chart);

    newGraphSpec.y.splice(newGraphSpec.y.indexOf(column), 1);

    WranglerStore.dispatch({
      type: WranglerActions.editChart,
      payload: {
        chart: newGraphSpec
      }
    });
  }

  addYValue(columnToAdd) {
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

  onYChange(e) {
    const column = e.target.value;
    const checked = e.target.checked;

    if (checked) {
      this.addYValue(column);
    } else {
      this.removeYValue(column);
    }
  }

  toggleShowAllX() {
    this.setState({showAllX: !this.state.showAllX});
  }

  toggleShowAllY() {
    this.setState({showAllY: !this.state.showAllY});
  }

  renderXOptions() {
    const graph = this.props.chart;

    const showMoreToggle = (
      <a href="#" onClick={this.toggleShowAllX}>
        {this.state.showAllX ? 'Show Less' : 'Show More'}
      </a>
    );

    let xList = this.state.columns;

    if (!this.state.showAllX) {
      xList = xList.slice(0,4);
    }

    return (
      <div>
        <div className="x-axis-options">
          <div className="radio">
            <label>
              <input
                type="radio"
                value="##"
                checked={'##' === graph.x}
                onChange={e => this.onXChange(e, graph)}
              />
              #
            </label>
          </div>
          {
            xList.map((column) => {
              return (
                <div
                  className="radio"
                  key={column}
                >
                  <label>
                    <input
                      type="radio"
                      value={column}
                      checked={column === graph.x}
                      onChange={e => this.onXChange(e, graph)}
                    />
                    {column}
                  </label>
                </div>
              );
            })
          }
        </div>

        {this.state.columns.length > 4 ? showMoreToggle : null}
      </div>
    );
  }

  renderYOptions() {
    const graph = this.props.chart;
    const showMoreToggle = (
      <a href="#" onClick={this.toggleShowAllY}>
        {this.state.showAllY ? 'Show Less' : 'Show More'}
      </a>
    );

    let yList = this.state.columns;
    if (!this.state.showAllY) {
      yList = yList.slice(0,5);
    }

    return (
      <div>
        <div className="y-axis-options">
          {
            yList.map((column) => {
              return (
                <div
                  className="checkbox"
                  key={column}
                >
                  <label>
                    <input
                      type="checkbox"
                      value={column}
                      checked={graph.y.indexOf(column) !== -1}
                      onChange={e => this.onYChange(e, graph)}
                    />
                    {column}
                  </label>
                </div>
              );
            })
          }
        </div>

        {this.state.columns.length > 5 ? showMoreToggle : null}
      </div>
    );
  }

  renderPanelContent() {
    if (!this.state.isExpanded) { return null; }

    const graph = this.props.chart;

    let chart;
    if (graph.x && graph.y.length > 0) {
      chart = <Charts spec={graph} />;
    }

    return (
      <div className="graph-content">
        <div className="graph-diagram">
          {chart}
        </div>

        <div className="x-axis-selector">
          <h5>X Axis</h5>

          {this.renderXOptions()}
        </div>

        <div className="y-axis-selector">
          <h5>Y Axis</h5>

          {this.renderYOptions()}
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
