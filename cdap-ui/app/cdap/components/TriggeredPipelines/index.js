/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import CollapsibleSidebar from 'components/CollapsibleSidebar';
import NamespaceStore from 'services/NamespaceStore';
import TriggeredPipelineRow from 'components/TriggeredPipelines/TriggeredPipelineRow';
import {setTriggeredPipelines, togglePipeline} from 'components/TriggeredPipelines/store/TriggeredPipelineActionCreator';
import {Provider, connect} from 'react-redux';
import TriggeredPipelineStore from 'components/TriggeredPipelines/store/TriggeredPipelineStore';
import T from 'i18n-react';

const PREFIX = `features.TriggeredPipelines`;

require('./TriggeredPipelines.scss');

const mapStateToProps = (state) => {
  return {
    triggeredPipelines: state.triggered.triggeredPipelines,
    expanded: state.triggered.expandedPipeline,
    pipelineInfo: state.triggered.expandedPipelineInfo,
    pipelineInfoLoading: state.triggered.pipelineInfoLoading
  };
};

class TriggeredPipelinesView extends Component {
  constructor(props) {
    super(props);

    this.state = {
      tabText: `${PREFIX}.collapsedTabLabel`
    };

    this.onToggle = this.onToggle.bind(this);
    this.onToggleSidebar = this.onToggleSidebar.bind(this);
  }

  componentWillMount() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    setTriggeredPipelines(namespace, this.props.pipelineName);
  }

  onToggleSidebar(isExpanded) {
    this.setState({
      tabText: isExpanded ? `${PREFIX}.expandedTabLabel` : `${PREFIX}.collapsedTabLabel`
    });
  }

  onToggle(pipeline) {
    togglePipeline(pipeline);
  }

  render() {
    let count = this.props.triggeredPipelines.length;
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
            this.props.triggeredPipelines.length === 0 ?
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
                    this.props.triggeredPipelines.map((pipeline) => {
                      return (
                        <TriggeredPipelineRow
                          isExpanded={`${pipeline.namespace}_${pipeline.application}` === this.props.expanded}
                          pipeline={pipeline}
                          onToggle={this.onToggle}
                          loading={this.props.pipelineInfoLoading}
                          pipelineInfo={this.props.pipelineInfo}
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

TriggeredPipelinesView.propTypes = {
  pipelineName: PropTypes.string.isRequired,
  triggeredPipelines: PropTypes.array,
  expanded: PropTypes.string,
  pipelineInfo: PropTypes.object,
  pipelineInfoLoading: PropTypes.bool
};

const TriggeredPipelinesConnect = connect(
  mapStateToProps
)(TriggeredPipelinesView);

export default function TriggeredPipelines({pipelineName}) {
  return (
    <Provider store={TriggeredPipelineStore}>
      <TriggeredPipelinesConnect
        pipelineName={pipelineName}
      />
    </Provider>
  );
}

TriggeredPipelines.propTypes = {
  pipelineName: PropTypes.string.isRequired
};
