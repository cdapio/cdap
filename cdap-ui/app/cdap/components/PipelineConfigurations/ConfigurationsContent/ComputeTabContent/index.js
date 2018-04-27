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
import React, { Component } from 'react';
import ProfilesListViewInPipeline, {PROFILE_NAME_PREFERENCE_PROPERTY} from 'components/PipelineConfigurations/ConfigurationsContent/ComputeTabContent/ProfilesListView';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import findIndex from 'lodash/findIndex';
import uuidV4 from 'uuid/v4';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import {connect} from 'react-redux';
import {objectQuery} from 'services/helpers';

class ComputeTabContent extends Component {

  static propTypes = {
    selectedProfile: PropTypes.object
  };

  onProfileSelect = (profileName) => {
    let {runtimeArgs} = PipelineConfigurationsStore.getState();
    let pairs = [...runtimeArgs.pairs];
    let existingProfile = findIndex(pairs, (pair) => pair.key === PROFILE_NAME_PREFERENCE_PROPERTY);
    if (existingProfile === -1) {
      pairs.push({
        key: PROFILE_NAME_PREFERENCE_PROPERTY,
        value: profileName,
        uniqueId: 'id-' + uuidV4()
      });
    } else {
      pairs[existingProfile].value = profileName;
    }
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
      payload: { runtimeArgs: { pairs } }
    });
    updatePipelineEditStatus();
  };
  render() {
    return (
      <div className="compute-tab-content">
        <ProfilesListViewInPipeline
          onProfileSelect={this.onProfileSelect}
          selectedProfile={this.props.selectedProfile}
          tableTitle={"Select the compute profile you want to use to run this pipeline"}
        />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  let selectedProfile = state.runtimeArgs.pairs.find(pair => pair.key === 'system.profile.name');
  let selectedProfileObj = {
    name: objectQuery(selectedProfile, 'value') || null
  };
  return {
    selectedProfile: selectedProfileObj
  };
};

const ConnectedComputeTabContent = connect(mapStateToProps)(ComputeTabContent);

export default ConnectedComputeTabContent;
