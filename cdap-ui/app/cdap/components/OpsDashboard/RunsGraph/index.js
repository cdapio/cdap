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
import Legends from 'components/OpsDashboard/RunsGraph/Legends';
import TypeSelector from 'components/OpsDashboard/RunsGraph/TypeSelector';
import {Observable} from 'rxjs/Observable';
import IconSVG from 'components/IconSVG';
import {next, prev} from 'components/OpsDashboard/store/ActionCreator';
import RunsTable from 'components/OpsDashboard/RunsGraph/RunsTable';
import {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';
import classnames from 'classnames';
import {getData, setLast24Hours, setIs7DaysAgo} from 'components/OpsDashboard/store/ActionCreator';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import T from 'i18n-react';

const PREFIX = 'features.OpsDashboard.RunsGraph';

require('./RunsGraph.scss');

const RUNS_GRAPH_CONTAINER = 'runs-graph-container';
const GRAPH_HEIGHT = 265;

class RunsGraphView extends Component {
  static propTypes = {
    loading: PropTypes.bool,
    data: PropTypes.array,
    displayType: PropTypes.oneOf(['chart', 'table']),
    viewByOption: PropTypes.string,
    changeDisplay: PropTypes.func,
    isLast24Hours: PropTypes.bool,
    is7DaysAgo: PropTypes.bool,
    namespacesPick: PropTypes.array
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
    setLast24Hours(true);
    setIs7DaysAgo(false);
    getData();
  };

  renderGraph(props = this.props) {
    let containerElem = document.getElementById(RUNS_GRAPH_CONTAINER);

    let width = containerElem.offsetWidth;
    renderGraph('#runs-graph', width, GRAPH_HEIGHT, props.data, props.viewByOption);
  }

  renderDisabledArrowToolitp(showTooltip, arrowId) {
    if (!showTooltip) {
      return null;
    }

    return (
      <UncontrolledTooltip
        placement="right"
        delay={0}
        target={arrowId}
        className="disabled-arrow-tooltip"
      >
        {T.translate(`${PREFIX}.disabledArrowTooltip`)}
      </UncontrolledTooltip>
    );
  }

  render() {
    const chart = (
      <div id={RUNS_GRAPH_CONTAINER}>
        <svg id="runs-graph" />
      </div>
    );

    const table = <RunsTable />;
    const arrowLeftId = "runs-graph-arrow-left";

    return (
      <div className="runs-graph-container">
        <div className="top-panel">
          <div className="title-and-subtitle">
            <span className="title">
              {T.translate(`${PREFIX}.header`)}
            </span>
            <div className="subtitle">
              {T.translate(`${PREFIX}.subtitle`, {
                context: this.props.namespacesPick.length + 1
              })}
            </div>
          </div>

          <TypeSelector />

          <div className="display-picker">
            <div className="time-picker">
              <div
                className={classnames({
                  "active": this.props.isLast24Hours
                })}
                onClick={!this.props.isLast24Hours ? this.last24Hour : undefined}
              >
                {T.translate(`${PREFIX}.last24Hours`)}
              </div>
            </div>

            <div className="display-type">
              <span
                className={classnames("option", {
                  "active": this.props.displayType === 'chart'
                })}
                onClick={this.props.changeDisplay.bind(this, 'chart')}
              >
                {T.translate(`${PREFIX}.chart`)}
              </span>
              <span className="separator">|</span>
              <span
                className={classnames("option", {
                  "active": this.props.displayType === 'table'
                })}
                onClick={this.props.changeDisplay.bind(this, 'table')}
              >
                {T.translate(`${PREFIX}.table`)}
              </span>
            </div>
          </div>
        </div>

        <div className="graph-container">
          {this.props.displayType === 'chart' ? chart : table}

          <div
            className={classnames("navigation arrow-left", {
              "disabled": this.props.is7DaysAgo
            })}
            onClick={!this.props.is7DaysAgo && prev}
            id={arrowLeftId}
          >
            <IconSVG name="icon-chevron-left" />
          </div>
          <div
            className="navigation arrow-right"
            onClick={next}
          >
            <IconSVG name="icon-chevron-right" />
          </div>
          {this.renderDisabledArrowToolitp(this.props.is7DaysAgo, arrowLeftId)}
        </div>

        {this.props.displayType === 'chart' ? <Legends /> : null}
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.dashboard.loading,
    data: state.dashboard.data,
    displayType: state.dashboard.displayType,
    viewByOption: state.dashboard.viewByOption,
    isLast24Hours: state.dashboard.isLast24Hours,
    is7DaysAgo: state.dashboard.is7DaysAgo,
    namespacesPick: state.namespaces.namespacesPick
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
