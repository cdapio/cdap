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
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {renderGraph} from 'components/OpsDashboard/RunsGraph/graphRenderer';
import ToggleRunsList from 'components/OpsDashboard/RunsGraph/ToggleRunsList';
import Legends from 'components/OpsDashboard/RunsGraph/Legends';
import TypeSelector from 'components/OpsDashboard/RunsGraph/TypeSelector';
import {Observable} from 'rxjs/Observable';
import IconSVG from 'components/IconSVG';
import {next, prev} from 'components/OpsDashboard/store/ActionCreator';

require('./RunsGraph.scss');

const RUNS_GRAPH_CONTAINER = 'runs-graph-container';
const GRAPH_HEIGHT = 230;

class RunsGraphView extends Component {
  static propTypes = {
    loading: PropTypes.bool,
    data: PropTypes.array,
    legends: PropTypes.object
  };

  componentDidMount() {
    // update graph on window resize
    this.windowResize$ = Observable.fromEvent(window, 'resize')
      .debounceTime(500)
      .subscribe(() => {
        this.renderGraph();
      });
  }

  componentWillReceiveProps(nextProps) {
    this.renderGraph(nextProps);
  }

  componentWillUnmount() {
    if (this.windowResize$ && this.windowResize$.unsubscribe) {
      this.windowResize$.unsubscribe();
    }
  }

  renderGraph(props = this.props) {
    let containerElem = document.getElementById(RUNS_GRAPH_CONTAINER);

    let width = containerElem.offsetWidth;
    renderGraph('#runs-graph', width, GRAPH_HEIGHT, props.data, props.legends);
  }

  render() {
    return (
      <div className="runs-graph-container">
        <div className="top-panel">
          <div className="title">
            Runs Timeline
          </div>

          <TypeSelector />

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

        <div className="graph-container">
          <div id={RUNS_GRAPH_CONTAINER}>
            <svg id="runs-graph" />
          </div>

          <div
            className="navigation arrow-left"
            onClick={prev}
          >
            <IconSVG name="icon-chevron-left" />
          </div>
          <div
            className="navigation arrow-right"
            onClick={next}
          >
            <IconSVG name="icon-chevron-right" />
          </div>
        </div>

        <Legends />

        <ToggleRunsList />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.dashboard.loading,
    data: state.dashboard.data,
    legends: state.legends
  };
};

const RunsGraph = connect(
  mapStateToProps
)(RunsGraphView);

export default RunsGraph;
