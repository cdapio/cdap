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
import shortid from 'shortid';

const initialState = {
  list: [],
  activeEntity: undefined,
  filter: '*',
  loading: true,
  isError: false
};

const market = (state=initialState, action) => {
  switch (action.type) {
    case 'SET_ENTITIES':
      return Object.assign({}, state, {
        list: action.payload.map(entity => Object.assign(entity, {id: shortid.generate()})),
        loading: false
      });
    case 'SET_ACTIVE_ENTITY':
      return Object.assign({}, state, {
        activeEntity: action.payload.entityId
      });
    case 'SET_FILTER':
      return Object.assign({}, state, { filter: action.payload });
    case 'SET_ERROR':
      return Object.assign({}, state, { isError: true, loading: false });
    case 'RESET':
      return Object.assign({}, initialState);
    default:
      return state;
  }
};

const createMarketStore = () => {
  return createStore(market);
};

const MarketStore = createMarketStore();
export default MarketStore;
