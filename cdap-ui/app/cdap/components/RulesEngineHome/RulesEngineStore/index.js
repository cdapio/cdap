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

const DEFAULTRULEBOOKSSTATE = {
  list: [],
  activeRulebookId: null,
  activeRulebookRules: []
};
const DEFAULTRULESENGINESTATE = {
  rulebooks: DEFAULTRULEBOOKSSTATE,
  rules: []
};

const RULESENGINEACTIONS = {
  SETRULEBOOKS: 'SETRULEBOOKS',
  SETRULESFORACTIVERULEBOOK: 'SETRULESFORACTIVERULEBOOK',
  SETRULES: 'SETRULES',
  SETACTIVERULEBOOK: 'SETACTIVERULEBOOK'
};

const rulebooks = (state = DEFAULTRULEBOOKSSTATE, action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETRULEBOOKS:
      return Object.assign({}, state, {
        list: action.payload.rulebooks,
        activeRulebookId: action.payload.rulebooks[0].id
      });
    case RULESENGINEACTIONS.SETACTIVERULEBOOK:
      return Object.assign({}, state, {
        activeRulebookId: action.payload.activeRulebook
      });
    case RULESENGINEACTIONS.SETRULESFORACTIVERULEBOOK:
      return Object.assign({}, state,{
        activeRulebookRules: action.payload.rules
      });
    default:
      return state;
  }
};

const rules = (state = [], action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETRULES:
      return action.payload.rules || state;
    default:
      return state;
  }
};

const RulesEngineStore = createStore(
  combineReducers({
    rulebooks,
    rules
  }),
  DEFAULTRULESENGINESTATE,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default RulesEngineStore;
export {RULESENGINEACTIONS};
