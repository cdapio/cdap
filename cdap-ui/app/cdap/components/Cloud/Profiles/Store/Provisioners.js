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
import { MyCloudApi } from 'api/cloud';
import isEmpty from 'lodash/isEmpty';
import { Observable } from 'rxjs/Observable';

const ACTIONS = {
  SAVE_PROVISIONERS: 'SAVE_PROVISIONERS',
};
const DEFAULT_STATE = {
  nameToLabelMap: {},
  list: [],
};

export const fetchProvisioners = () => {
  let returnObservable$ = MyCloudApi.getProvisioners();
  returnObservable$.subscribe(
    (provisioners) => {
      store.dispatch({
        type: ACTIONS.SAVE_PROVISIONERS,
        payload: {
          provisioners,
        },
      });
    },
    (err) => {
      console.log('Fetching provisioners failed: ', err);
    }
  );
  return returnObservable$;
};

const provisioners = (state = DEFAULT_STATE, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SAVE_PROVISIONERS: {
      let { provisioners } = action.payload;
      let provisionersMap = {};
      provisioners.forEach((provisioner) => {
        provisionersMap[provisioner.name] = provisioner.label;
      });
      return {
        nameToLabelMap: provisionersMap,
        list: provisioners,
      };
    }
    default:
      return state;
  }
};

export const getProvisionersMap = () => {
  let { list } = store.getState();
  const observableReturn = () =>
    Observable.create((observer) => {
      observer.next(store.getState());
    });
  if (isEmpty(list)) {
    return fetchProvisioners().flatMap(observableReturn);
  } else {
    return observableReturn();
  }
};

const store = createStore(provisioners, DEFAULT_STATE, composeEnhancers('ProvisionersMapStore')());

export default store;
