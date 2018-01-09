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
import PipelineTableRow from 'components/PipelineList/DeployedPipelineView/PipelineTable/PipelineTableRow';
import T from 'i18n-react';

require('./PipelineTable.scss');

const PREFIX = 'features.PipelineList';

export default class PipelineTable extends Component {
  static propTypes = {
    pipelineList: PropTypes.array,
    pipelineInfo: PropTypes.object
  };

  render() {
    return (
      <div className="pipeline-list-table">
        <div className="table-header">
          <div className="table-column toggle-expand-column"></div>
          <div className="table-column name">
            {T.translate(`${PREFIX}.pipelineName`)}
          </div>
          <div className="table-column type">
            {T.translate(`${PREFIX}.type`)}
          </div>
          <div className="table-column version">
            {T.translate(`${PREFIX}.version`)}
          </div>
          <div className="table-column runs">
            {T.translate(`${PREFIX}.runs`)}
          </div>
          <div className="table-column status">
            {T.translate(`${PREFIX}.status`)}
          </div>
          <div className="table-column last-start">
            {T.translate(`${PREFIX}.lastStartTime`)}
          </div>
          <div className="table-column next-run">
            {T.translate(`${PREFIX}.nextRun`)}
          </div>
          <div className="table-column tags">
            {T.translate(`${PREFIX}.tags`)}
          </div>
          <div className="table-column action"></div>
        </div>

        <div className="table-body">
          {
            this.props.pipelineList.map((pipelineName) => {
              return (
                <PipelineTableRow
                  key={pipelineName}
                  pipelineInfo={this.props.pipelineInfo[pipelineName]}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}
