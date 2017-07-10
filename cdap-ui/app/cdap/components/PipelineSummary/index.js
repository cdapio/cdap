/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import {fetchSummary} from 'components/PipelineSummary/Store/PipelineSummaryActions';
import PipelineSummaryStore from 'components/PipelineSummary/Store/PipelineSummaryStore';
import {convertProgramToApi} from 'services/program-api-converter';
import RunsHistoryGraph from 'components/PipelineSummary/RunsHistoryGraph';
import LogsMetricsGraph from 'components/PipelineSummary/LogsMetricsGraph';
import NodesMetricsGraph from 'components/PipelineSummary/NodesMetricsGraph';
import { DropdownToggle, DropdownItem } from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import {MyPipelineApi} from 'api/pipeline';
import {humanReadableDuration} from 'services/helpers';
import isNil from 'lodash/isNil';
import Mousetrap from 'mousetrap';
import ee from 'event-emitter';

const PREFIX = 'features.PipelineSummary';

require('./PipelineSummary.scss');
const ONE_DAY_SECONDS = 86400;

export default class PipelineSummary extends Component {
  constructor(props) {
    super(props);
    const RUNSFILTERPREFIX = `${PREFIX}.runsFilter`;
    let {namespaceId, appId, programType, programId, pipelineConfig} = props;
    this.state = {
      runs: [],
      logsMetrics: [],
      nodesMetrics: [],
      totalRunsCount: props.totalRunsCount,
      runsLimit: 10,
      filterType: 'limit',
      activeRunsFilter: T.translate(`${RUNSFILTERPREFIX}.last10Runs`),
      loading: true,
      nodeMetricsLoading: true,
      start: null,
      end: null,
      avgRunTime: '--',
      nodesMap: {}
    };
    this.fetchRunsByLimit = this.fetchRunsByLimit.bind(this);
    this.fetchRunsByTime = this.fetchRunsByTime.bind(this);
    this.runsDropdown = [
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last10Runs`),
        onClick: this.fetchRunsByLimit.bind(this, 10)
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last50Runs`),
        onClick: this.fetchRunsByLimit.bind(this, 50)
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last100Runs`),
        onClick: this.fetchRunsByLimit.bind(this, 100)
      },
      {
        label: 'divider'
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last1Day`),
        onClick: this.fetchRunsByTime.bind(this, ONE_DAY_SECONDS)
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last7Days`),
        onClick: this.fetchRunsByTime.bind(this, ONE_DAY_SECONDS * 7)
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.last30Days`),
        onClick: this.fetchRunsByTime.bind(this, ONE_DAY_SECONDS * 30)
      },
      {
        label: T.translate(`${RUNSFILTERPREFIX}.sinceInception`),
        onClick: () => {
          this.setState({
            activeRunsFilter: T.translate(`${RUNSFILTERPREFIX}.sinceInception`),
            filterType: 'time',
            start: null,
            end: null
          });
          fetchSummary({
            namespaceId,
            appId,
            programType: convertProgramToApi(programType),
            programId,
            pipelineConfig
          });
        }
      }
    ];
    fetchSummary({
      namespaceId,
      appId,
      programType: convertProgramToApi(programType),
      programId,
      pipelineConfig,
      limit: this.state.runsLimit
    });
    this.eventEmitter = ee(ee);
  }
  componentWillReceiveProps(nextProps) {
    if (nextProps.totalRunsCount !== this.state.totalRunsCount) {
      this.setState({
        totalRunsCount: nextProps.totalRunsCount
      });
    }
  }
  componentWillUnmount() {
    Mousetrap.unbind('esc');
  }
  componentDidMount() {
    let {namespaceId: namespace, appId, programId: workflowId} = this.props;
    MyPipelineApi.pollStatistics({
      namespace,
      appId,
      workflowId
    })
      .subscribe(
        res => {
          if (typeof res !== 'object') {
            return;
          }
          this.setState({
            avgRunTime: res.avgRunTime
          });
        }
      );
    Mousetrap.bind('esc', () => {
      this.eventEmitter.emit('CLOSE_HINT_TOOLTIP');
      this.setState({
        currentHoveredElement: null
      });
    });
    this.storeSubscription = PipelineSummaryStore.subscribe(() => {
      let {runs, loading, nodesMap, nodeMetricsLoading} = PipelineSummaryStore.getState().pipelinerunssummary;
      let logsMetrics = runs.map(run => ({
        runid: run.runid,
        logsMetrics: run.logsMetrics || {},
        start: run.start,
        end: run.end
      }));
      runs = runs.map(run => ({
        runid: run.runid,
        duration: run.duration,
        start: run.start,
        end: run.end,
        status: run.status
      }));
      let state = {
        runs,
        logsMetrics,
        nodesMap,
        loading,
        nodeMetricsLoading
      };
      if (this.state.filterType === 'time') {
        const getStartAndEnd = () => {
          let start, end;
          // Will happen if chosen 'Since Inception' as UI doesn't know a start and end beforehand.
          if (isNil(this.state.start) || isNil(this.state.end)) {
            end = runs[0].start;
            start = runs[runs.length - 1].start;
          }
          return !isNil(start) && !isNil(end) ? {start, end} : {};
        };

        state = Object.assign({}, state, getStartAndEnd() , {
          limit: runs.length
        });
      }
      this.setState(state);
    });
  }
  fetchRunsByLimit(limit, filterLabel) {
    this.setState({
      runsLimit: limit,
      activeRunsFilter: filterLabel,
      filterType: 'limit',
      start: null,
      end: null
    });
    let {namespaceId, appId, programType, programId, pipelineConfig} = this.props;
    fetchSummary({
      namespaceId,
      appId,
      programType: convertProgramToApi(programType),
      programId,
      pipelineConfig,
      limit
    });
  }
  fetchRunsByTime(time, filterLabel) {
    let end = Math.floor(Date.now() / 1000);
    let start = end - time;
    this.setState({
      activeRunsFilter: filterLabel,
      filterType: 'time',
      runsLimit: null,
      start,
      end
    });
    let {namespaceId, appId, programType, programId, pipelineConfig} = this.props;
    fetchSummary({
      namespaceId,
      appId,
      programType: convertProgramToApi(programType),
      programId,
      pipelineConfig,
      start,
      end
    });
  }
  renderTitleBar() {
    return (
      <div className="top-title-bar">
        <div> {T.translate(`${PREFIX}.title`)}</div>
        <div className="stats-container text-xs-right">
          <span>
            <strong>{T.translate(`${PREFIX}.statsContainer.avgRunTime`)}: </strong>
            {
              humanReadableDuration(this.state.avgRunTime)
            }
          </span>
          <span>
            <strong>{T.translate(`${PREFIX}.statsContainer.totalRuns`)}: </strong>
            {this.state.totalRunsCount}
          </span>
          <IconSVG
            name="icon-close"
            onClick={this.props.onClose}
          />
        </div>
      </div>
    );
  }
  render() {
    return (
      <div className="pipeline-summary">
        {
          this.renderTitleBar()
        }
        <div className="filter-container">
          <span> {T.translate(`${PREFIX}.filterContainer.view`)} </span>
          <UncontrolledDropdown className="runs-dropdown">
            <DropdownToggle caret>
              <span>{this.state.activeRunsFilter}</span>
              <IconSVG name="icon-caret-down" />
            </DropdownToggle>
            <CustomDropdownMenu>
              {
                this.runsDropdown.map(dropdown => {
                  if (dropdown.label === 'divider') {
                    return (
                      <DropdownItem
                        tag="li"
                        divider
                      />
                    );
                  }
                  return (
                    <DropdownItem
                      tag="li"
                      onClick={dropdown.onClick.bind(this, dropdown.label)}
                    >
                      {
                        dropdown.label
                      }
                    </DropdownItem>
                  );
                })
              }
            </CustomDropdownMenu>
          </UncontrolledDropdown>
        </div>
        <div className="graphs-container">
          <RunsHistoryGraph
            activeFilterLabel={this.state.activeRunsFilter}
            totalRunsCount={this.state.totalRunsCount}
            runs={this.state.runs}
            runsLimit={this.state.runsLimit}
            start={this.state.start}
            end={this.state.end}
            xDomainType={this.state.filterType}
            runContext={this.props}
            isLoading={this.state.loading}
          />
          <LogsMetricsGraph
            activeFilterLabel={this.state.activeRunsFilter}
            totalRunsCount={this.state.totalRunsCount}
            runs={this.state.logsMetrics}
            runsLimit={this.state.runsLimit}
            start={this.state.start}
            end={this.state.end}
            xDomainType={this.state.filterType}
            runContext={this.props}
            isLoading={this.state.loading}
          />
          <NodesMetricsGraph
            activeFilterLabel={this.state.activeRunsFilter}
            totalRunsCount={this.state.totalRunsCount}
            runs={this.state.nodesMetrics}
            runsLimit={this.state.runsLimit}
            start={this.state.start}
            end={this.state.end}
            xDomainType={this.state.filterType}
            runContext={this.props}
            isLoading={this.state.nodeMetricsLoading}
            recordType="recordsin"
            nodesMap={this.state.nodesMap['sources']}
          />
          <NodesMetricsGraph
            activeFilterLabel={this.state.activeRunsFilter}
            totalRunsCount={this.state.totalRunsCount}
            runs={this.state.nodesMetrics}
            runsLimit={this.state.runsLimit}
            start={this.state.start}
            end={this.state.end}
            xDomainType={this.state.filterType}
            runContext={this.props}
            isLoading={this.state.nodeMetricsLoading}
            recordType="recordsout"
            nodesMap={this.state.nodesMap['sinks']}
          />
        </div>
      </div>
    );
  }
}

PipelineSummary.propTypes = {
  namespaceId: PropTypes.string.isRequired,
  appId: PropTypes.string.isRequired,
  programType: PropTypes.string.isRequired,
  programId: PropTypes.string.isRequired,
  pipelineConfig: PropTypes.object.isRequired,
  totalRunsCount: PropTypes.number,
  onClose: PropTypes.func
};
