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

import {createStore, combineReducers} from 'redux';
import {defaultAction} from 'services/helpers';
import {MyServiceProviderApi} from 'api/serviceproviders';
const SYSTEMSERVICESACTIONS = {
  SETSERVICES: 'SETSERVICES',
  SETERROR: 'SETERROR'
};

const initialSystemServices = {
  list: [],
  __error: false
};
const services = (state = initialSystemServices, action = defaultAction) => {
  switch (action.type) {
    case SYSTEMSERVICESACTIONS.SETSERVICES:
      return Object.assign({}, state, {
        list: action.payload.services || [],
        __error: false
      });
    case SYSTEMSERVICESACTIONS.SETERROR:
      return Object.assign({}, state, {
        __error: action.payload.error
      });
    default:
      return state;
  }
};

const ServicesStore = createStore(
  combineReducers({
    services
  }),
  {
    services: initialSystemServices
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);
let systemServicesPollSubscription;
const pollSystemServices = () => {
  systemServicesPollSubscription = MyServiceProviderApi
    .pollServicesList()
    .subscribe(
      services => ServicesStore.dispatch({
        type: SYSTEMSERVICESACTIONS.SETSERVICES,
        payload: {
          services
        }
      }),
      err => ServicesStore.dispatch({
        type: SYSTEMSERVICESACTIONS.SETERROR,
        payload: {error: err}
      })
    );
};

const stopSystemServicesPolling = () => {
  systemServicesPollSubscription();
};

export default ServicesStore;
export {pollSystemServices, stopSystemServicesPolling};
export {SYSTEMSERVICESACTIONS};
