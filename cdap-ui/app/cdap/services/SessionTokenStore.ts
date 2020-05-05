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

import { createStore, Reducer } from 'redux';
import LoadingIndicatorStore, {
  BACKENDSTATUS,
} from 'components/LoadingIndicator/LoadingIndicatorStore';
import { IAction } from 'services/redux-helpers';
import { composeEnhancers } from 'services/helpers';
import Cookies from 'universal-cookie';

const cookie = new Cookies();
const DEFAULT_STATE = '';
const SESSION_TOKENS_ACTIONS = {
  SET_TOKEN: 'SET_TOKEN',
};

const reducer: Reducer<string> = (state: string = DEFAULT_STATE, action: IAction): string => {
  switch (action.type) {
    case SESSION_TOKENS_ACTIONS.SET_TOKEN:
      return action.payload.sessionToken;
    default:
      return state;
  }
};

const store = createStore(reducer, DEFAULT_STATE, composeEnhancers('SessionTokenStore')());

export async function fetchSessionToken() {
  try {
    const headers: HeadersInit = {
      'X-Requested-With': 'XMLHttpRequest',
    };
    if (window.CDAP_CONFIG.securityEnabled && cookie.get('CDAP_Auth_Token')) {
      headers.authorization = `Bearer ${cookie.get('CDAP_Auth_Token')}`;
    }
    const sessionTokenRes = await fetch('/sessionToken', { headers });
    const sessionToken = await sessionTokenRes.text();
    store.dispatch({
      type: SESSION_TOKENS_ACTIONS.SET_TOKEN,
      payload: {
        sessionToken,
      },
    });
    return Promise.resolve(sessionToken);
  } catch (e) {
    LoadingIndicatorStore.dispatch({
      type: BACKENDSTATUS.NODESERVERDOWN,
    });
    return Promise.reject('');
  }
}

export default store;
