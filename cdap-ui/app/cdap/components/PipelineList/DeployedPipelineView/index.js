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
import {getCurrentNamespace} from 'services/NamespaceStore';
import {MyPipelineApi} from 'api/pipeline';
import {Observable} from 'rxjs/Observable';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';
import IconSVG from 'components/IconSVG';
import StatusMapper from 'services/StatusMapper';
import PipelineTable from 'components/PipelineList/DeployedPipelineView/PipelineTable';
import GLOBALS from 'services/global-constants';

require('./DeployedPipelineView.scss');

const INFO_MAP = GLOBALS.programInfo;

const PREFIX = 'features.PipelineList';

export default class DeployedPipelineView extends Component {
  componentWillMount() {
    this.fetchApps();
  }

  state = {
    pipelineList: [],
    pipelineInfo: {}
  };

  fetchApps() {
    let namespace = getCurrentNamespace();

    let params = {
      namespace,
      artifactName: 'cdap-data-pipeline,cdap-data-streams'
    };

    MyPipelineApi.list(params)
      .subscribe(this.fetchRuns.bind(this));
  }

  fetchRuns(pipelines) {
    let namespace = getCurrentNamespace();

    let reqArr = [];

    pipelines.forEach((pipeline) => {
      let info = INFO_MAP[pipeline.artifact.name];

      let params = {
        namespace,
        appId: pipeline.name,
        version: pipeline.version,
        programType: info.programType,
        programName: info.programName
      };

      reqArr.push(MyPipelineApi.getRuns(params));
    });

    Observable.combineLatest(reqArr)
      .subscribe((res) => {
        pipelines.forEach((pipeline, index) => {
          pipeline.runs = res[index];
        });

        this.collapseRuns(pipelines);
      });
  }

  collapseRuns(pipelines) {
    let pipelineInfo = {};

    pipelines.forEach((pipeline) => {
      if (!pipelineInfo[pipeline.name]) {
        pipelineInfo[pipeline.name] = {
          name: pipeline.name,
          type: T.translate(`${PREFIX}.${pipeline.artifact.name}`),
          runs: [],
          running: []
        };
      }

      let pipelineObj = pipelineInfo[pipeline.name];

      pipelineObj.runs = pipelineObj.runs.concat(pipeline.runs);
      pipelineObj.version = pipeline.version;
    });

    let pipelineList = Object.keys(pipelineInfo);

    pipelineList.forEach((pipelineName) => {
      let runs = pipelineInfo[pipelineName].runs;
      if (!runs.length) {
        pipelineInfo[pipelineName].status = StatusMapper.lookupDisplayStatus('Deployed');
        return;
      }

      runs = orderBy(pipelineInfo[pipelineName].runs, ['start'], ['desc']);

      pipelineInfo[pipelineName].runs = runs;
      pipelineInfo[pipelineName].lastStartTime = runs[0].start;

      let running = runs.filter((run) => run.status === 'RUNNING');
      let status =  StatusMapper.lookupDisplayStatus(runs[0].status);

      pipelineInfo[pipelineName].status = status;
      pipelineInfo[pipelineName].running = running;
    });

    this.setState({
      pipelineList,
      pipelineInfo
    });
  }

  render() {
    return (
      <div className="pipeline-deployed-view pipeline-list-content">
        <div className="deployed-header">
          <div className="pipeline-count">
            {T.translate(`${PREFIX}.DeployedPipelineView.pipelineCount`, {count: this.state.pipelineList.length})}
          </div>

          <div className="search-box">
            <div className="input-group">
              <span className="input-group-addon">
                <IconSVG name="icon-search" />
              </span>
              <input
                type="text"
                className="form-control"
                placeholder={T.translate(`${PREFIX}.DeployedPipelineView.searchPlaceholder`)}
              />
            </div>
          </div>
        </div>

        <PipelineTable
          pipelineList={this.state.pipelineList}
          pipelineInfo={this.state.pipelineInfo}
        />
      </div>
    );
  }
}
