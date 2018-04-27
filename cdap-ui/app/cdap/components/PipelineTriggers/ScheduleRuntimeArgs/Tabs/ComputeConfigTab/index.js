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

import PropTypes from 'prop-types';
import React from 'react';
import T from 'i18n-react';
import {connect} from 'react-redux';
import ProfilesListViewInPipeline from 'components/PipelineConfigurations/ConfigurationsContent/ComputeTabContent/ProfilesListView';
import {setSelectedProfile} from 'components/PipelineTriggers/ScheduleRuntimeArgs/ScheduleRuntimeArgsActions';

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs.Tabs.ComputeConfig';

function ComputeConfigTab({triggeringPipelineId, selectedProfile, disabled}) {
  return (
    <div className="compute-config-tab">
      {
        disabled ?
          null
        :
          <h4>{T.translate(`${PREFIX}.title`, {triggeringPipelineId})} </h4>
      }
      <ProfilesListViewInPipeline
        selectedProfile={selectedProfile}
        onProfileSelect={setSelectedProfile}
        disabled={disabled}
      />
    </div>
  );
}
ComputeConfigTab.propTypes = {
  triggeringPipelineId: PropTypes.string,
  selectedProfile: PropTypes.object,
  disabled: PropTypes.bool
};

const mapStateToProps = (state) => {
  return {
    triggeringPipelineId: state.args.triggeringPipelineInfo.id,
    selectedProfile: state.args.selectedProfile,
    disabled: state.args.disabled
  };
};

const ConnectedComputeConfigTab = connect(mapStateToProps)(ComputeConfigTab);

export default ConnectedComputeConfigTab;
