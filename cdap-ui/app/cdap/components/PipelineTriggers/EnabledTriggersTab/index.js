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
import {connect} from 'react-redux';
import EnabledTriggerRow from 'components/PipelineTriggers/EnabledTriggersTab/EnabledTriggerRow';
import T from 'i18n-react';

const TRIGGER_PREFIX = 'features.PipelineTriggers';
const PREFIX = `${TRIGGER_PREFIX}.EnabledTriggers`;

require('./EnabledTriggersTab.scss');

const mapStateToProps = (state) => {
  return {
    enabledTriggers: state.triggers.enabledTriggers,
    pipelineName: state.triggers.pipelineName,
    expandedTrigger: state.triggers.expandedTrigger
  };
};

function EnabledTriggersView({enabledTriggers, pipelineName}) {
  return (
    <div className="existing-triggers-tab">
      <div className="pipeline-trigger-header">
        {T.translate(`${PREFIX}.title`, {pipelineName})}
      </div>

      <div className="pipeline-count">
        {
          T.translate(`${PREFIX}.pipelineCount`, {
            context: {
              count: enabledTriggers.length
            }
          })
        }
      </div>

      {
        enabledTriggers.length === 0 ?
          null
        :
          (
            <div className="pipeline-list">
              <div className="pipeline-list-header">
                <div className="caret-container"></div>
                <div className="pipeline-name">
                  {T.translate(`${TRIGGER_PREFIX}.pipelineName`)}
                </div>
                <div className="namespace">
                  {T.translate(`${TRIGGER_PREFIX}.namespace`)}
                </div>
              </div>

              {
                enabledTriggers.map((schedule) => {
                  const mapTriggerRowStateToProps = (state) => {
                    return {
                      schedule,
                      isExpanded: state.triggers.expandedTrigger === schedule.name,
                      loading: state.enabledTriggers.loading,
                      info: state.enabledTriggers.pipelineInfo,
                      pipelineName
                    };
                  };

                  const TriggerRow = connect(
                    mapTriggerRowStateToProps
                  )(EnabledTriggerRow);

                  return <TriggerRow />;
                })
              }
            </div>
          )
      }
    </div>
  );
}

EnabledTriggersView.propTypes = {
  enabledTriggers: PropTypes.array,
  pipelineName: PropTypes.string,
  expandedTrigger: PropTypes.string,
  toggleExpandTrigger: PropTypes.func
};

const EnabledTriggersTab = connect(
  mapStateToProps
)(EnabledTriggersView);

export default EnabledTriggersTab;
