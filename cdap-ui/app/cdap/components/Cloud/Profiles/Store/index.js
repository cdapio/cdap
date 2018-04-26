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

import {createStore} from 'redux';
import {defaultAction, composeEnhancers} from 'services/helpers';

const PROFILES_ACTIONS = {
  SET_PROFILES: 'SET_PROFILES',
  SET_LOADING: 'SET_LOADING',
  SET_ERROR: 'SET_ERROR',
  RESET: 'RESET'
};

const DEFAULT_PROFILES_STATE = {
  profiles: [],
  loading: false,
  error: null,
};

const profiles = (state = DEFAULT_PROFILES_STATE, action = defaultAction) => {
  switch (action.type) {
    case PROFILES_ACTIONS.SET_PROFILES:
      return {
        ...state,
        profiles: action.payload.profiles,
        error: null,
        loading: false
      };
    case PROFILES_ACTIONS.SET_LOADING:
      return {
        ...state,
        error: null,
        loading: action.payload.loading
      };
    case PROFILES_ACTIONS.SET_ERROR:
      return {
        ...state,
        error: action.payload.error,
        loading: false
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
export {PROFILES_ACTIONS};
