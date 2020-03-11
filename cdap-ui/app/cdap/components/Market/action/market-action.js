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

import MarketStore from '../store/market-store.js';

function setList(list) {
  MarketStore.dispatch({
    type: 'SET_ENTITIES',
    payload: list,
  });
}

function setFilter(filter) {
  MarketStore.dispatch({
    type: 'SET_FILTER',
    payload: filter,
  });
}

function setError() {
  MarketStore.dispatch({
    type: 'SET_ERROR',
  });
}

function setSelectedMarketHost(host) {
  MarketStore.dispatch({
    type: 'SET_SELECTED_MARKET_HOST',
    payload: host,
  });
}

const MarketAction = {
  setList,
  setFilter,
  setError,
  setSelectedMarketHost,
};

export default MarketAction;
