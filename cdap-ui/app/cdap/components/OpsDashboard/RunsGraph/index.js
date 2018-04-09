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
import RunsTable from 'components/OpsDashboard/RunsGraph/RunsTable';
import {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';
import classnames from 'classnames';
import {getData} from 'components/OpsDashboard/store/ActionCreator';

require('./RunsGraph.scss');

const RUNS_GRAPH_CONTAINER = 'runs-graph-container';
const GRAPH_HEIGHT = 230;

class RunsGraphView extends Component {
  static propTypes = {
    loading: PropTypes.bool,
    data: PropTypes.array,
    legends: PropTypes.object,
    displayType: PropTypes.oneOf(['chart', 'table']),
    changeDisplay: PropTypes.func
  };

  componentDidMount() {
    this.windowResizeSubscribe();
  }

  componentDidUpdate() {
    if (this.props.displayType === 'table') {
      this.windowResizeUnsubscribe();
    } else {
      this.windowResizeSubscribe();
      this.renderGraph();
    }
  }

  componentWillUnmount() {
    this.windowResizeUnsubscribe();
  }

  windowResizeSubscribe() {
    // update graph on window resize
    this.windowResize$ = Observable.fromEvent(window, 'resize')
      .debounceTime(500)
      .subscribe(() => {
        this.renderGraph();
      });
  }

  windowResizeUnsubscribe() {
    if (this.windowResize$ && this.windowResize$.unsubscribe) {
      this.windowResize$.unsubscribe();
    }
  }

  last24Hour = () => {
    getData();
  };

  renderGraph(props = this.props) {
    let containerElem = document.getElementById(RUNS_GRAPH_CONTAINER);

    let width = containerElem.offsetWidth;
    renderGraph('#runs-graph', width, GRAPH_HEIGHT, props.data, props.legends);
  }

  render() {
    const chart = (
      <div id={RUNS_GRAPH_CONTAINER}>
        <svg id="runs-graph" />
      </div>
    );

    const table = <RunsTable />;

    return (
      <div className="runs-graph-container">
        <div className="top-panel">
          <div className="title">
            Runs Timeline
          </div>

          <TypeSelector />

          <div className="display-picker">
            <div className="time-picker">
              <div onClick={this.last24Hour}>
                Last 24 hours
              </div>
            </div>

            <div className="display-type">
              <span
                className={classnames('option', {
                  'active': this.props.displayType === 'chart'
                })}
                onClick={this.props.changeDisplay.bind(this, 'chart')}
              >
                Chart
              </span>
              <span className="separator">|</span>
              <span
                className={classnames('option', {
                  'active': this.props.displayType === 'table'
                })}
                onClick={this.props.changeDisplay.bind(this, 'table')}
              >
                Table
              </span>
            </div>
          </div>
        </div>

        <div className="graph-container">
          {this.props.displayType === 'chart' ? chart : table}

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

        {this.props.displayType === 'chart' ? <Legends /> : null}

        <ToggleRunsList />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.dashboard.loading,
    data: state.dashboard.data,
    legends: state.legends,
    displayType: state.dashboard.displayType
  };
};

const mapDispatch = (dispatch) => {
  return {
    changeDisplay: (displayType) => {
      dispatch({
        type: DashboardActions.changeDisplayType,
        payload: {
          displayType
        }
      });
    }
  };
};

const RunsGraph = connect(
  mapStateToProps,
  mapDispatch
)(RunsGraphView);

export default RunsGraph;
