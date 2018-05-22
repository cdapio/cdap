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
import ProfilesListViewInPipeline, {PROFILE_NAME_PREFERENCE_PROPERTY, PROFILE_PROPERTIES_PREFERENCE} from 'components/PipelineDetails/ProfilesListView';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import {connect} from 'react-redux';
import {objectQuery, convertKeyValuePairsToMap, convertMapToKeyValuePairs} from 'services/helpers';

class ComputeTabContent extends Component {

  static propTypes = {
    selectedProfile: PropTypes.object
  };

  onProfileSelect = (profileName, customizations = {}) => {
    let {runtimeArgs} = PipelineConfigurationsStore.getState();
    let pairs = [...runtimeArgs.pairs];
    let runtimeObj = convertKeyValuePairsToMap(pairs, true);
    let existingProfile = runtimeObj[PROFILE_NAME_PREFERENCE_PROPERTY];
    if (!existingProfile) {
      runtimeObj[PROFILE_NAME_PREFERENCE_PROPERTY] = profileName;
      Object.keys(customizations).forEach(profileProp => {
        let key = `${PROFILE_PROPERTIES_PREFERENCE}.${profileProp}`;
        runtimeObj[key] = customizations[profileProp];
      });
    } else {
      if (existingProfile !== profileName) {
        // If the profile is not the same remove any
        // customizations applied to the previous profile.
        Object.keys(runtimeObj).forEach(runtimearg => {
          if (runtimearg.indexOf(PROFILE_PROPERTIES_PREFERENCE) !== -1) {
            delete runtimeObj[runtimearg];
          }
        });
      }
      runtimeObj[PROFILE_NAME_PREFERENCE_PROPERTY] = profileName;
      Object.keys(customizations).forEach(profileProperty => {
        let key = `${PROFILE_PROPERTIES_PREFERENCE}.${profileProperty}`;
        runtimeObj[key] = customizations[profileProperty];
      });
    }
    // This is required to not override macros that are marked as provided and nonDeletable.
    let newRunTimePairs = convertMapToKeyValuePairs(runtimeObj);
    newRunTimePairs = newRunTimePairs.map(newRunTimeArg => {
      let existingPair = pairs.find(p => p.key === newRunTimeArg.key && p.value === newRunTimeArg.value) || {};
      return {
        ...existingPair,
        ...newRunTimeArg,
      };
    });
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
      payload: { runtimeArgs: { pairs: newRunTimePairs } }
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
  let selectedProfile = state.runtimeArgs.pairs.find(pair => pair.key === PROFILE_NAME_PREFERENCE_PROPERTY);
  let profileCustomizations = state.runtimeArgs.pairs.filter(pair => pair.key.indexOf(PROFILE_PROPERTIES_PREFERENCE) !== -1);
  let customizationsMap = {};
  profileCustomizations.forEach(customProp => {
    let propName = customProp.key.replace(`${PROFILE_PROPERTIES_PREFERENCE}.`, '');
    customizationsMap[propName] = customProp.value;
  });
  let selectedProfileObj = {
    name: objectQuery(selectedProfile, 'value') || null,
    profileCustomizations: customizationsMap
  };
  return {
    selectedProfile: selectedProfileObj
  };
};

const ConnectedComputeTabContent = connect(mapStateToProps)(ComputeTabContent);

export default ConnectedComputeTabContent;
