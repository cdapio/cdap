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

import { createStore } from 'redux';
import { defaultAction, composeEnhancers } from 'services/helpers';

const PROFILES_ACTIONS = {
  SET_PROFILES: 'SET_PROFILES',
  SET_DEFAULT_PROFILE: 'SET_DEFAULT_PROFILE',
  SET_NEW_PROFILE: 'SET_NEW_PROFILE',
  SET_PROFILE_METRICS: 'SET_PROFILE_METRICS',
  SET_SCHEDULES_TRIGGERS_COUNT: 'SET_SCHEDULES_TRIGGERS_COUNT',
  SET_LOADING: 'SET_LOADING',
  SET_ERROR: 'SET_ERROR',
  RESET: 'RESET',
};

const DEFAULT_PROFILES_STATE = {
  profiles: [],
  defaultProfile: null,
  newProfile: '',
  loading: false,
  error: null,
};

const PROFILE_STATUSES = {
  ENABLED: 'enabled',
  DISABLED: 'disabled',
};

const profiles = (state = DEFAULT_PROFILES_STATE, action = defaultAction) => {
  switch (action.type) {
    case PROFILES_ACTIONS.SET_PROFILES:
      return {
        ...state,
        profiles: action.payload.profiles,
        error: null,
        loading: false,
      };
    case PROFILES_ACTIONS.SET_PROFILE_METRICS: {
      let { profilesToMetricsMap } = action.payload;
      let { profiles } = state;
      profiles = profiles.map((profile) => {
        let metricObj = { runs: '--', minutes: '--' };
        let profileKey = `${profile.scope}:${profile.name}`;
        let profileMetrics = profilesToMetricsMap[profileKey] || {};
        /*
          We are adding empty oneday and overall metrics AND metrics from backend
          as we are not sure UI will get all metrics from backend. This will give
          a default value of '--' for those that are unavailable.
        */
        return {
          ...profile,
          oneDayMetrics: metricObj,
          overAllMetrics: metricObj,
          ...profileMetrics,
        };
      });
      return {
        ...state,
        profiles,
      };
    }
    case PROFILES_ACTIONS.SET_SCHEDULES_TRIGGERS_COUNT: {
      let { profile: profileToUpdate, schedulesCount, triggersCount } = action.payload;
      let { profiles } = state;
      profiles = profiles.map((profile) => {
        if (profile.name === profileToUpdate) {
          return {
            ...profile,
            schedulesCount,
            triggersCount,
          };
        }
        return profile;
      });
      return {
        ...state,
        profiles,
      };
    }
    case PROFILES_ACTIONS.SET_DEFAULT_PROFILE:
      return {
        ...state,
        defaultProfile: action.payload.defaultProfile,
      };
    case PROFILES_ACTIONS.SET_NEW_PROFILE:
      return {
        ...state,
        newProfile: action.payload.newProfile,
      };
    case PROFILES_ACTIONS.SET_LOADING:
      return {
        ...state,
        error: null,
        loading: action.payload.loading,
      };
    case PROFILES_ACTIONS.SET_ERROR:
      return {
        ...state,
        error: action.payload.error,
        loading: false,
      };
    case PROFILES_ACTIONS.RESET:
      return DEFAULT_PROFILES_STATE;
    default:
      return state;
  }
};

const ProfilesStore = createStore(
  profiles,
  DEFAULT_PROFILES_STATE,
  composeEnhancers('ProfilesStore')()
);

export default ProfilesStore;
export { PROFILES_ACTIONS, PROFILE_STATUSES };
