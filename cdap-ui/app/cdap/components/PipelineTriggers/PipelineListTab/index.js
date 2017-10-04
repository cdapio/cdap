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

import PropTypes from 'prop-types';

import React from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {changeNamespace} from 'components/PipelineTriggers/store/PipelineTriggersActionCreator';
import {connect} from 'react-redux';
import PipelineTriggersActions from 'components/PipelineTriggers/store/PipelineTriggersActions';
import PipelineTriggersRow from 'components/PipelineTriggers/PipelineListTab/PipelineTriggersRow';
import T from 'i18n-react';

const TRIGGER_PREFIX = 'features.PipelineTriggers';
const PREFIX = `${TRIGGER_PREFIX}.SetTriggers`;

require('./PipelineListTab.scss');

const mapStateToProps = (state) => {
  return {
    pipelineList: state.triggers.pipelineList,
    selectedNamespace: state.triggers.selectedNamespace,
    pipelineName: state.triggers.pipelineName,
    expandedPipeline: state.triggers.expandedPipeline
  };
};

const mapDispatch = (dispatch) => {
  return {
    toggleExpandPipeline: (pipeline) => {
      dispatch({
        type: PipelineTriggersActions.setExpandedPipeline,
        payload: { expandedPipeline : pipeline }
      });
    }
  };
};

function PipelineListTabView({pipelineList, pipelineName, selectedNamespace, expandedPipeline, toggleExpandPipeline}) {
  let namespaceList = NamespaceStore.getState().namespaces;
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  let triggeredPipelineInfo = {
    id: pipelineName,
    namespace
  };
  function changeNamespaceEvent(e) {
    changeNamespace(e.target.value);
  }

  return (
    <div className="pipeline-list-tab">
      <div className="pipeline-trigger-header">
        {T.translate(`${PREFIX}.title`, {pipelineName})}
      </div>

      <div className="namespace-selector">
        <span>{T.translate(`${PREFIX}.viewNamespace`)}</span>

        <div className="namespace-selector-dropdown">
          <select
            className="form-control"
            value={selectedNamespace}
            onChange={changeNamespaceEvent}
          >
            {
              namespaceList.map((namespace) => {
                return (
                  <option
                    value={namespace.name}
                    key={namespace.name}
                  >
                    {namespace.name}
                  </option>
                );
              })
            }
          </select>
        </div>
      </div>

      <div className="pipeline-count">
        {T.translate(`${PREFIX}.pipelineCount`, {count: pipelineList.length})}
      </div>

      {
        pipelineList.length === 0 ?
          null
        :
          (
            <div className="pipeline-list">
              <div className="pipeline-list-header">
                <div className="caret-container"></div>
                <div className="pipeline-name">
                  {T.translate(`${TRIGGER_PREFIX}.pipelineName`)}
                </div>
              </div>
              {
                pipelineList.map((pipeline) => {
                  let triggeringPipelineInfo = {
                    id: pipeline.name,
                    namespace: selectedNamespace,
                    description: pipeline.description
                  };
                  return (
                    <PipelineTriggersRow
                      key={pipeline.name}
                      pipelineRow={pipeline.name}
                      isExpanded={expandedPipeline === pipeline.name}
                      onToggle={toggleExpandPipeline}
                      triggeringPipelineInfo={triggeringPipelineInfo}
                      triggeredPipelineInfo={triggeredPipelineInfo}
                      selectedNamespace={selectedNamespace}
                    />
                  );
                })
              }
            </div>
          )
      }
    </div>
  );
}

PipelineListTabView.propTypes = {
  pipelineList: PropTypes.array,
  selectedNamespace: PropTypes.string,
  pipelineName: PropTypes.string,
  expandedPipeline: PropTypes.string,
  toggleExpandPipeline: PropTypes.bool
};

const PipelineListTab = connect(
  mapStateToProps,
  mapDispatch
)(PipelineListTabView);

export default PipelineListTab;
