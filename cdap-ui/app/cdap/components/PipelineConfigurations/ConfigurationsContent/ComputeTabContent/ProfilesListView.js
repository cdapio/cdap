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

import React, {Component} from 'react';
import {MyProfileApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import {MyPreferenceApi} from 'api/preference';
import {Observable} from 'rxjs/Observable';
import PipelineDetailStore from 'components/PipelineDetails/store';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {updatePipelineEditStatus} from 'components/PipelineConfigurations/Store/ActionCreator';
import findIndex from 'lodash/findIndex';
import uuidV4 from 'uuid/v4';

require('./ListViewInPipeline.scss');

export const PROFILE_NAME_PREFERENCE_PROPERTY = 'system.profile.name';
export default class ProfilesListViewInPipeline extends Component {

  state = {
    profiles: [],
    loading: true,
    selectedProfile: null
  };

  componentWillMount() {
    let appId = PipelineDetailStore.getState().name;
    Observable.forkJoin(
      MyProfileApi.list({
        namespace: getCurrentNamespace()
      }),
      MyPreferenceApi.getAppPreferences({
        namespace: getCurrentNamespace(),
        appId
      })
    )
      .subscribe(
        ([profiles = [], preferences = {}]) => {
          let selectedProfile = preferences[PROFILE_NAME_PREFERENCE_PROPERTY];
          this.setState({
            loading: false,
            profiles,
            selectedProfile
          });
        },
        (err) => {
          console.log('ERROR in fetching profiles from backend: ', err);
        }
      );
  }

  onProfileSelect = (profileName) => {
    this.setState({
      selectedProfile: profileName
    });
    let {runtimeArgs} = PipelineConfigurationsStore.getState();
    let pairs = [...runtimeArgs.pairs];
    pairs = pairs.filter(pair => pair.key.length);
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

  renderGridHeader = () => {
    return (
      <div className="grid-header">
        <div className="grid-item">
          <div></div>
          <strong>Profile Name</strong>
          <strong>Provider</strong>
          <strong>Scope</strong>
        </div>
      </div>
    );
  };

  renderGridBody = () => {
    return (
      <div className="grid-body">
        {
          this.state.profiles.map(profile => {
            return (
              <div
                className={classnames("grid-item", {
                  "active": this.state.selectedProfile === profile.name
                })}
                onClick={this.onProfileSelect.bind(this, profile.name)}
              >
                <div>
                  {
                    this.state.selectedProfile === profile.name ? (
                      <IconSVG name="icon-check" className="text-success" />
                    ) : null
                  }
                </div>
                <div>{profile.name}</div>
                <div>{profile.provisionerInfo.name}</div>
                <div>{profile.scope}</div>
              </div>
            );
          })
        }
      </div>
    );
  };

  renderGrid = () => {
    if (!this.state.profiles.length) {
      return (
        <div>
          <strong> No Profiles created </strong>
          <div>
            <a href={`/cdap/ns/${getCurrentNamespace()}/create-profile`}>
              Click here
            </a>
            <span> to create one </span>
          </div>
        </div>
      );
    }
    return (
      <div>
        <strong> Select the compute profile you want to use to run this pipeline</strong>
        <div className="profiles-count text-right">{this.state.profiles.length} Compute Profiles</div>
        <div className="grid grid-container">
          {this.renderGridHeader()}
          {this.renderGridBody()}
        </div>
      </div>
    );
  };

  render() {
    if (this.state.loading) {
      return (
        <div>
          <LoadingSVG />
        </div>
      );
    }
    return (
      <div className="profiles-list-view-on-pipeline">
        {this.renderGrid()}
      </div>
    );
  }
}
