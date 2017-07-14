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
import NodesRecordsGraph from 'components/PipelineSummary/NodesRecordsGraph';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import {DropdownToggle, DropdownItem} from 'reactstrap';
import IconSVG from 'components/IconSVG';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {objectQuery} from 'services/helpers';
import T from 'i18n-react';
import classnames from 'classnames';
import {getGraphHeight} from 'components/PipelineSummary/RunsGraphHelpers';
import SortableStickyTable from 'components/SortableStickyTable';
import CopyableRunID from 'components/PipelineSummary/CopyableRunID';
import moment from 'moment';
import EmptyMessageContainer from 'components/PipelineSummary/EmptyMessageContainer';

require('./NodesMetricsGraph.scss');

const PREFIX = 'features.PipelineSummary.nodesMetricsGraph';
const GRAPHPREFIX = `features.PipelineSummary.graphs`;

const getTableHeaders = (recordType) => ([
  {
    label: T.translate(`${PREFIX}.${recordType}.table.headers.runCount`),
    property: 'index'
  },
  {
    label: T.translate(`${PREFIX}.${recordType}.table.headers.inputrecords`),
    property: 'numberOfRecords'
  },
  {
    label: T.translate(`${PREFIX}.${recordType}.table.headers.startTime`),
    property: 'start'
  }
]);

export default class NodesMetricsGraph extends Component {
  constructor(props) {
    super(props);
    this.state = {
      nodesMap: props.nodesMap,
      activeNodeRuns: [],
      viewState: 'chart',
      activeNode: null
    };
    this.tableHeaders = getTableHeaders(props.recordType);
    this.constructData = this.constructData.bind(this);
    this.setActiveNode = this.setActiveNode.bind(this);
    this.renderTable = this.renderTable.bind(this);
    this.renderTableBody = this.renderTableBody.bind(this);
    this.constructData();
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      nodesMap: nextProps.nodesMap,
      activeNode: null,
      activeNodeRuns: []
    }, this.constructData);
  }
  constructData() {
    if (!Object.keys(this.props.nodesMap).length) {
      return;
    }
    let activeNode = Object.keys(this.props.nodesMap).pop();
    let activeNodeRuns = this.props.nodesMap[activeNode].map((run, i) => Object.assign({}, run, {index: i + 1}));
    this.setState({
      activeNode,
      activeNodeRuns
    });
  }
  setActiveNode(activeNode) {
    let activeNodeRuns = objectQuery(this.state.nodesMap, activeNode).map((run, i) => Object.assign({}, run, {index: i + 1}));
    this.setState({
      activeNode,
      activeNodeRuns
    });
  }
  renderNodesDropdown() {
    if (!Object.keys(this.state.nodesMap)) {
      return null;
    }
    let nodes = Object.keys(this.state.nodesMap);
    return (
      <UncontrolledDropdown className="runs-dropdown">
        <DropdownToggle caret>
          <span>{this.state.activeNode}</span>
          <IconSVG name="icon-caret-down" />
        </DropdownToggle>
        <CustomDropdownMenu>
          {
            nodes
              .map(node => {
                return (
                  <DropdownItem
                    tag="li"
                    onClick={this.setActiveNode.bind(this, node)}
                  >
                    {node}
                  </DropdownItem>
                );
              })
          }
        </CustomDropdownMenu>
      </UncontrolledDropdown>
    );
  }
  renderEmptyMessage() {
    return (
      <EmptyMessageContainer
        xDomainType={this.props.xDomainType}
        label={this.props.activeFilterLabel}
      />
    );
  }
  renderLoading() {
    return (<EmptyMessageContainer loading={true} />);
  }
  renderTableBody(records) {
    return (
      <table className="table">
        <tbody>
          {
            records.map(record => {
              return (
                <tr>
                  <td>
                    <span>{record.index}</span>
                    <CopyableRunID
                      runid={record.runid}
                      idprefix={`nodes-metrics-${this.props.recordType}`}
                    />
                  </td>
                  <td>
                    <span>{record.numberOfRecords}</span>
                  </td>
                  <td> {moment(record.start * 1000).format('llll')}</td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  }
  renderTable() {
    return (
      <SortableStickyTable
        tableHeaders={this.tableHeaders}
        entities={this.state.activeNodeRuns.map((run, i) => Object.assign({}, run, {index: i + 1}))}
        renderTableBody={this.renderTableBody}
      />
    );
  }
  renderContent() {
    if (this.props.isLoading) {
      return this.renderLoading();
    }
    if (!this.state.activeNodeRuns.length) {
      return this.renderEmptyMessage();
    }
    if (this.state.viewState === 'chart') {
      let height = getGraphHeight(this.containerRef);
      return (
        <NodesRecordsGraph
          records={this.state.activeNodeRuns}
          graphHeight={height}
          activeNode={this.state.activeNode}
          {...this.props}
        />
      );
    }
    if (this.state.viewState === 'table') {
      return this.renderTable();
    }
  }
  render() {
    return (
      <div
        className="nods-metrics-graph"
        ref={ref => this.containerRef = ref}
      >
        <div className="title-container">
          <div className="title">{T.translate(`${PREFIX}.${this.props.recordType}.title`)} </div>
          <div className="viz-switcher">
            {this.renderNodesDropdown()}
            <span
              className={classnames("chart", {"active": this.state.viewState === 'chart'})}
              onClick={() => this.setState({viewState: 'chart'})}
            >
              {T.translate(`${GRAPHPREFIX}.vizSwitcher.chart`)}
            </span>
            <span
              className={classnames({"active": this.state.viewState === 'table'})}
              onClick={() => this.setState({viewState: 'table'})}
            >
              {T.translate(`${GRAPHPREFIX}.vizSwitcher.table`)}
            </span>
          </div>
        </div>
        {
          this.renderContent()
        }
      </div>
    );
  }
}
NodesMetricsGraph.defaultProps = {
  nodesMap: {}
};
NodesMetricsGraph.propTypes = {
  totalRunsCount: PropTypes.number,
  runsLimit: PropTypes.number,
  xDomainType: PropTypes.oneOf(['limit', 'time']),
  runContext: PropTypes.object,
  isLoading: PropTypes.bool,
  start: PropTypes.number,
  end: PropTypes.number,
  recordType: PropTypes.oneOf(['recordsin', 'recordsout']),
  nodesMap: PropTypes.object,
  activeFilterLabel: PropTypes.string
};
