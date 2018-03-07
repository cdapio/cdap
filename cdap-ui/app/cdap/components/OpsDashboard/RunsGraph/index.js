/*
 * Copyright © 2018 Cask Data, Inc.
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
import {renderGraph} from 'components/OpsDashboard/RunsGraph/graphRenderer';
import IconSVG from 'components/IconSVG';
import MyOperationsApi from 'api/operations';
import {getCurrentNamespace} from 'services/NamespaceStore';
import moment from 'moment';
import {parseDashboardData} from 'components/OpsDashboard/RunsGraph/DataParser';
import ToggleRunsList from 'components/OpsDashboard/RunsGraph/ToggleRunsList';

require('./RunsGraph.scss');

const RUNS_GRAPH_CONTAINER = 'runs-graph-container';

export default class ClassName extends Component {
  state = {
    loading: true,
    data: [],
    pipelineCount: 0,
    customAppCount: 0
  };

  componentDidMount() {
    this.getData();
  }

  getData() {
    let start = moment().subtract(24, 'h').format('x'),
        duration = 1440;

    start = parseInt(start, 10);

    let params = {
      start,
      duration, // 24 hours in minutes
      namespace: getCurrentNamespace()
    };

    MyOperationsApi.getDashboard(params)
      .subscribe((res) => {
        let {
          pipelineCount,
          customAppCount,
          buckets
        } = parseDashboardData(res, start, duration);

        let data = Object.keys(buckets).map((time) => {
          return {
            ...buckets[time],
            time
          };
        });

        this.setState({
          pipelineCount,
          customAppCount,
          data,
          loading: false
        });

        // Render Graph
        let containerElem = document.getElementById(RUNS_GRAPH_CONTAINER);

        let width = containerElem.offsetWidth,
            height = 230;
        renderGraph('#runs-graph', width, height, this.state.data);
      });
  }

  render() {
    return (
      <div className="runs-graph-container">
        <div className="top-panel">
          <div className="title">
            Runs Timeline
          </div>

          <div className="type-selector">
            <div className="type-item">
              <IconSVG name="icon-check-square" />
              <span>Pipelines ({this.state.pipelineCount})</span>
            </div>

            <div className="type-item">
              <IconSVG name="icon-check-square" />
              <span>Custom Apps ({this.state.customAppCount})</span>
            </div>
          </div>

          <div className="display-picker">
            <div className="time-picker">
              <div>Last 24 hours</div>
            </div>

            <div className="display-type">
              <span className="active">Chart</span>
              <span className="separator">|</span>
              <span>Table</span>
            </div>
          </div>
        </div>

        <div className="runs-graph-container">
          <div id={RUNS_GRAPH_CONTAINER}>
            <svg id="runs-graph" />
          </div>
        </div>

        <div className="legends">
          <div className="start-method-legend">
            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="manual"
              />
              <span>Manually started runs</span>
            </div>

            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="schedule"
              />
              <span>Scheduled/triggered runs</span>
            </div>
          </div>

          <div className="status-legend">
            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="running"
              />
              <span>Running</span>
            </div>

            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="successful"
              />
              <span>Successful runs</span>
            </div>

            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="failed"
              />
              <span>Failed runs</span>
            </div>
          </div>

          <div className="delay-legend">
            <div className="select-item">
              <IconSVG name="icon-check-square" />
              <IconSVG
                name="icon-circle"
                className="delay"
              />
              <span>Delay between starting and running</span>
            </div>
          </div>
        </div>

        <ToggleRunsList />
      </div>
    );
  }
}
