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

const ACTIONS = {
  SET_JSON_SPEC: 'SET_JSON_SPEC',
  SET_LOADING: 'SET_LOADING',
  SET_ERROR: 'SET_ERROR'
};

const DEFAULT_PROVISIONER_JSON_SPEC_MAP = {
  map: {},
  loading: true,
  error: null,
  selectedProvisioner: 'gce-dataproc'
};

const provisionerJsonSpecMap = (state = DEFAULT_PROVISIONER_JSON_SPEC_MAP, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SET_JSON_SPEC:
      return {
        ...state,
        error: null,
        map: {
          ...state.map,
          [action.payload.provisionerName]: action.payload.provisionerSpec
        },
        loading: false
      };
    case ACTIONS.SET_LOADING:
      return {
        ...state,
        error: null,
        loading: action.payload.loading
      };
    case ACTIONS.SET_ERROR:
      return {
        ...state,
        error: action.payload.error,
        loading: false
      };
    default:
      return state;
  }
};

const ProvisionerInfoStore = createStore(
  provisionerJsonSpecMap,
  DEFAULT_PROVISIONER_JSON_SPEC_MAP,
  composeEnhancers('ProvisionerSpecStore')()
);

export default ProvisionerInfoStore;
export {ACTIONS};
