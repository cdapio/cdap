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
import IconSVG from 'components/IconSVG';
import StatusMapper from 'services/StatusMapper';
import NextRun from 'components/PipelineList/DeployedPipelineView/NextRun';
import Duration from 'components/Duration';
import classnames from 'classnames';
import {humanReadableDate} from 'services/helpers';
import PipelineTags from 'components/PipelineList/DeployedPipelineView/PipelineTags';
import TimeDuration from 'components/PipelineList/DeployedPipelineView/TimeDuration';
import {getCurrentNamespace} from 'services/NamespaceStore';
import T from 'i18n-react';

const PREFIX = 'features.PipelineList';

export default class PipelineTableRow extends Component {
  static propTypes = {
    pipelineInfo: PropTypes.object
  };

  componentWillMount() {
    if (this.props.pipelineInfo && this.props.pipelineInfo.running.length > 0) {
      this.setState({
        expandable: true
      });
    }
  }

  state = {
    expandable: false,
    expanded: false
  };

  toggleExpand = () => {
    if (!this.state.expandable) { return; }
    this.setState({
      expanded: !this.state.expanded
    });
  };

  renderRunningPipeline() {
    if (!this.state.expanded) { return null; }

    let pipelineInfo = this.props.pipelineInfo;

    return (
      <div className="running-pipelines">
        <table className="table">
          <thead>
            <th>
              {T.translate(`${PREFIX}.startTime`)}
            </th>
            <th>
              {T.translate(`${PREFIX}.duration`)}
            </th>
          </thead>

          <tbody>
            {
              pipelineInfo.running.map((run) => {
                let namespace = getCurrentNamespace();

                let link = window.getHydratorUrl({
                  stateName: 'hydrator.detail',
                  stateParams: {
                    namespace,
                    pipelineId: pipelineInfo.name,
                    runid: run.runid
                  }
                });

                return (
                  <tr key={run.runid}>
                    <td>
                      <a href={link}>
                        {humanReadableDate(run.start)}
                      </a>
                    </td>
                    <td>
                      <a href={link}>
                        <TimeDuration startTime={run.start} />
                      </a>
                    </td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }

  render() {
    if (!this.props.pipelineInfo) { return null; }

    let pipelineInfo = this.props.pipelineInfo;
    let statusClassName = StatusMapper.getStatusIndicatorClass(pipelineInfo.status);
    let namespace = getCurrentNamespace();

    let pipelineLink = window.getHydratorUrl({
      stateName: 'hydrator.detail',
      stateParams: {
        namespace,
        pipelineId: pipelineInfo.name
      }
    });

    return (
      <div className={classnames('table-row-container', {
        expanded: this.state.expanded
      })}>
        <div className="table-row">
          <div
            className={classnames('table-column toggle-expand-column text-xs-center', {
              expandable: this.state.expandable
            })}
            onClick={this.toggleExpand}
          >
            {
              !this.state.expandable ? null :
              (
                <IconSVG name={this.state.expanded ? 'icon-caret-down' : 'icon-caret-right'} />
              )
            }
          </div>
          <div className="table-column name">
            <div className="pipeline-name">
              <a
                href={pipelineLink}
                title={pipelineInfo.name}
              >
                {pipelineInfo.name}
              </a>
            </div>
            {
              pipelineInfo.running.length === 0 ?
                null
              :
                (
                  <div className="pipeline-run">
                    {pipelineInfo.running.length} Running

                    {this.state.expanded ? T.translate(`${PREFIX}.selectOne`) : null}
                  </div>
                )
            }
          </div>
          <div className="table-column type">
            {pipelineInfo.type}
          </div>
          <div className="table-column version">
            {pipelineInfo.version}
          </div>
          <div className="table-column runs">
            {pipelineInfo.runs.length}
          </div>
          <div className="table-column status">
            <span className={`fa fa-fw ${statusClassName}`}>
              <IconSVG name="icon-circle" />
            </span>
            <span className="text">
              {pipelineInfo.status}
            </span>
          </div>
          <div className="table-column last-start">
            <Duration
              targetTime={pipelineInfo.lastStartTime}
              isMillisecond={false}
            />
          </div>
          <div className="table-column next-run">
            <NextRun pipelineInfo={pipelineInfo} />
          </div>
          <div className="table-column tags">
            <PipelineTags pipelineName={pipelineInfo.name} />
          </div>
          <div className="table-column action text-xs-center">
            <IconSVG name="icon-cog" />
          </div>
        </div>

        {this.renderRunningPipeline()}
      </div>
    );
  }
}
