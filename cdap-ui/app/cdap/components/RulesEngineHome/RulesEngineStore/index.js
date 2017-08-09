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
import {objectQuery} from 'services/helpers';

const DEFAULTRULEBOOKSSTATE = {
  list: null,
  activeRulebookId: null,
  activeRulebookRules: null,
  createRulebook: false
};
const DEFAULTRULESSTATE = {
  list: null,
  activeRuleId: null,
};

const DEFAULTERRORSTATE = {
  showError: false,
  message: null
};
const DEFAULTINTEGRATIONSTATE = {
  embedded: false,
  done: false
};

const DEFAULTRULESENGINESTATE = {
  rulebooks: DEFAULTRULEBOOKSSTATE,
  rules: DEFAULTRULESSTATE,
  integration: DEFAULTINTEGRATIONSTATE
};

const RULESENGINEACTIONS = {
  SETRULEBOOKS: 'SETRULEBOOKS',
  SETRULESFORACTIVERULEBOOK: 'SETRULESFORACTIVERULEBOOK',
  SETRULEBOOKSLOADING: 'SETRULEBOOKSLOADING',
  SETRULES: 'SETRULES',
  SETRULESLOADING: 'SETRULESLOADING',
  SETACTIVERULEBOOK: 'SETACTIVERULEBOOK',
  SETCREATERULEBOOK: 'SETCREATERULEBOOK',
  SETACTIVERULE: 'SETACTIVERULE',
  RESETACTIVERULE: 'RESETACTIVERULE',
  SETERROR: 'SETERROR',
  RESETERROR: 'RESETERROR',
  SETINTEGRATIONEMBEDDED: 'SETINTEGRATIONEMBEDDED',
  SETINTEGRATIONDONE: 'SETINTEGRATIONDONE'
};

const rulebooks = (state = DEFAULTRULEBOOKSSTATE, action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETRULEBOOKS:
      return Object.assign({}, state, {
        list: action.payload.rulebooks,
        activeRulebookId: objectQuery(action, 'payload', 'rulebooks', 0, 'id')
      });
    case RULESENGINEACTIONS.SETACTIVERULEBOOK:
      return Object.assign({}, state, {
        activeRulebookId: action.payload.activeRulebook,
        createRulebook: false
      });
    case RULESENGINEACTIONS.SETRULESFORACTIVERULEBOOK:
      return Object.assign({}, state,{
        activeRulebookRules: action.payload.rules,
        createRulebook: false
      });
    case RULESENGINEACTIONS.SETCREATERULEBOOK:
      return Object.assign({}, state, {
        createRulebook: action.payload.isCreate
      });
    case RULESENGINEACTIONS.SETRULEBOOKSLOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading
      });
    case RULESENGINEACTIONS.RESET:
      return DEFAULTRULEBOOKSSTATE;
    default:
      return state;
  }
};

const rules = (state = DEFAULTRULESSTATE, action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETRULES:
      return Object.assign({}, state, {
        list: action.payload.rules || state.list,
        loading: false
      });
    case RULESENGINEACTIONS.SETACTIVERULE:
      return Object.assign({}, state, {
        activeRuleId: action.payload.activeRuleId,
        loading: false
      });
    case RULESENGINEACTIONS.RESETACTIVERULE:
      return Object.assign({}, state, {
        activeRuleId: null
      });
    case RULESENGINEACTIONS.SETRULESLOADING:
      return Object.assign({}, state, {
        loading: true
      });
    case RULESENGINEACTIONS.RESET:
      return DEFAULTRULESSTATE;
    default:
      return state;
  }
};

const integration = (state = DEFAULTINTEGRATIONSTATE, action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETINTEGRATIONEMBEDDED:
      return Object.assign({}, state, {embedded: true});
    case RULESENGINEACTIONS.SETINTEGRATIONDONE:
      return Object.assign({}, state, {done: true});
    case RULESENGINEACTIONS.RESET:
      return DEFAULTINTEGRATIONSTATE;
    default:
      return state;
  }
};

const error = (state = DEFAULTERRORSTATE, action = defaultAction) => {
  switch (action.type) {
    case RULESENGINEACTIONS.SETERROR:
      return Object.assign({}, state, action.payload.error);
    case RULESENGINEACTIONS.RESETERROR:
      return DEFAULTERRORSTATE;
    case RULESENGINEACTIONS.RESET:
      return DEFAULTERRORSTATE;
    default:
      return state;
  }
};

const RulesEngineStore = createStore(
  combineReducers({
    rulebooks,
    rules,
    error,
    integration
  }),
  DEFAULTRULESENGINESTATE,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default RulesEngineStore;
export {RULESENGINEACTIONS};
