/*
 * Copyright © 2016 Cask Data, Inc.
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
import uuidV4 from 'uuid/v4';
import VersionRange from 'services/VersionRange';
import Version from 'services/VersionRange/Version';
import VersionStore from 'services/VersionStore';
import isNil from 'lodash/isNil';

const initialState = {
  list: [],
  activeEntity: undefined,
  filter: '*',
  loading: true,
  isError: false,
  displayCTA: true
};

const market = (state=initialState, action) => {
  switch (action.type) {
    case 'SET_ENTITIES':
      return Object.assign({}, state, {
        list: filterEntities(action.payload)
          .map(entity => Object.assign(entity, {id: uuidV4()})),
        loading: false
      });
    case 'SET_ACTIVE_ENTITY':
      return Object.assign({}, state, {
        activeEntity: action.payload.entityId,
        displayCTA: !isNil(action.payload.displayCTA) ? action.payload.displayCTA : true
      });
    case 'SET_FILTER':
      return Object.assign({}, state, {
        filter: action.payload,
        activeEntity: undefined
      });
    case 'SET_ERROR':
      return Object.assign({}, state, { isError: true, loading: false });
    case 'RESET':
      return Object.assign({}, initialState);
    default:
      return state;
  }
};

function filterEntities(list) {
  let store = window.CaskCommon ? window.CaskCommon.VersionStore : VersionStore;
  const cdapVersion = new Version(store.getState().version);

  let filteredList = list.filter((entity) => {
    if (!entity.cdapVersion) { return true; }

    const entityVersionRange = new VersionRange(entity.cdapVersion);
    return entityVersionRange.versionIsInRange(cdapVersion);
  });

  return filteredList;
}

const createMarketStore = () => {
  return createStore(market);
};

const MarketStore = createMarketStore();
export default MarketStore;
