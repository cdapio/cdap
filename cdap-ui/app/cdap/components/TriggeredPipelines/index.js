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

import React, { Component, PropTypes } from 'react';
import CollapsibleSidebar from 'components/CollapsibleSidebar';
import {MyScheduleApi} from 'api/schedule';
import NamespaceStore from 'services/NamespaceStore';
import TriggeredPipelineRow from 'components/TriggeredPipelines/TriggeredPipelineRow';
import {MyAppApi} from 'api/app';
import T from 'i18n-react';

const PREFIX = `features.TriggeredPipelines`;

require('./TriggeredPipelines.scss');

export default class TriggeredPipelines extends Component {
  constructor(props) {
    super(props);

    this.state = {
      triggeredPipelines: [],
      expanded: null,
      loading: false,
      pipelineInfo: null,
      tabText: `${PREFIX}.collapsedTabLabel`
    };

    this.onToggle = this.onToggle.bind(this);
    this.onToggleSidebar = this.onToggleSidebar.bind(this);
  }

  componentWillMount() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let params = {
      namespace,
      'trigger-namespace-id': namespace,
      'trigger-program-type': 'workflows',
      'trigger-app-name': this.props.pipelineName,
      'trigger-program-name': 'DataPipelineWorkflow'
    };

    MyScheduleApi.getTriggeredList(params)
      .subscribe((res) => {
        let triggeredPipelines = res.map((schedule) => {
          // doing this because currently the backend does not return the pipeline name
          let nameSplit = schedule.name.split('.');
          let obj = {
            pipelineName: nameSplit[0],
            namespace: nameSplit[1],
            schedule
          };

          return obj;
        });

        this.setState({triggeredPipelines});
      });
  }

  onToggleSidebar(isExpanded) {
    this.setState({
      tabText: isExpanded ? `${PREFIX}.expandedTabLabel` : `${PREFIX}.collapsedTabLabel`
    });
  }

  onToggle(pipeline) {
    if (!pipeline) {
      this.setState({expanded: null});
      return;
    }

    this.setState({
      loading: true,
      expanded: `${pipeline.namespace}_${pipeline.pipelineName}`
    });

    let namespace = NamespaceStore.getState().selectedNamespace;
    let params = {
      namespace,
      appId: this.props.pipelineName
    };

    MyAppApi.get(params)
      .subscribe((res) => {
        this.setState({
          loading: false,
          pipelineInfo: res
        });
      });
  }

  render() {
    let count = this.state.triggeredPipelines.length;
    let pipelineName = this.props.pipelineName;

    return (
      <CollapsibleSidebar
        position="right"
        toggleTabLabel={T.translate(`${this.state.tabText}`, {count})}
        backdrop={false}
        onToggle={this.onToggleSidebar}
      >
        <div className="triggered-pipeline-content">
          <div className="triggered-pipeline-header">
            {T.translate(`${PREFIX}.title`, {pipelineName})}
          </div>

          <div className="triggered-pipeline-count">
            {
              T.translate(`${PREFIX}.pipelineCount`, {
                context: {
                  count
                }
              })
            }
          </div>

          {
            this.state.triggeredPipelines.length === 0 ?
              null
            :
              (
                <div className="triggered-pipeline-list">
                  <div className="pipeline-list-header">
                    <div className="caret-container"></div>
                    <div className="pipeline-name">
                      {T.translate(`${PREFIX}.pipelineName`)}
                    </div>
                    <div className="namespace">
                      {T.translate(`${PREFIX}.namespace`)}
                    </div>
                  </div>
                  {
                    this.state.triggeredPipelines.map((pipeline) => {
                      return (
                        <TriggeredPipelineRow
                          isExpanded={`${pipeline.namespace}_${pipeline.pipelineName}` === this.state.expanded}
                          pipeline={pipeline}
                          onToggle={this.onToggle}
                          loading={this.state.loading}
                          pipelineInfo={this.state.pipelineInfo}
                          sourcePipeline={this.props.pipelineName}
                        />
                      );
                    })
                  }
                </div>
              )
          }
        </div>
      </CollapsibleSidebar>
    );
  }
}

TriggeredPipelines.propTypes = {
  pipelineName: PropTypes.string.isRequired,
  namespace: PropTypes.string.isRequired
};
