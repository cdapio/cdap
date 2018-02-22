/*
* Copyright © 2016-2017 Cask Data, Inc.
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

import {combineReducers, createStore} from 'redux';
import HostPortActions from 'components/DataPrepConnections/KafkaConnection/HostPortEditor/HostPortActions';
import uuidV4 from 'uuid/v4';

const defaultAction = {
  type: '',
  payload: {}
};

const initialState = {
  rows: []
};

const hostport = (state = initialState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case HostPortActions.setHost:
      stateCopy = Object.assign({}, state);
      if (action.payload.host === null || typeof action.payload.host === 'undefined') {
        return stateCopy;
      }
      stateCopy.rows[action.payload.index].host = action.payload.host;
      return stateCopy;
    case HostPortActions.setPort:
      stateCopy = Object.assign({}, state);
      if (action.payload.port === null || typeof action.payload.port === 'undefined') {
        return stateCopy;
      }
      stateCopy.rows[action.payload.index].port = action.payload.port;
      return stateCopy;

    case HostPortActions.addRow:
      stateCopy = Object.assign({}, state);
      stateCopy.rows.splice(action.payload.index + 1, 0, {
        host: '',
        port: '',
        uniqueId: uuidV4()
      });
      return stateCopy;
    case HostPortActions.deletePair:
      stateCopy = Object.assign({}, state);
      stateCopy.rows.splice(action.payload.index, 1);
      if (!stateCopy.rows.length) {
        stateCopy.rows.push({
          host: '',
          port: '',
          uniqueId: uuidV4()
        });
      }
      return stateCopy;
    case HostPortActions.onReset:
      return initialState;
    case HostPortActions.onUpdate:
      stateCopy = Object.assign({}, state);
      stateCopy.rows = action.payload.rows;
      return stateCopy;
    default:
      return state;
  }
};

const HostPortStore = createStore(
  combineReducers({
    hostport
  }),
  initialState,
  window.devToolsExtension ? window.devToolsExtension() : f => f
);

export default HostPortStore;
