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

import {combineReducers, createStore} from 'redux';
import DSVActions from 'components/DSVEditor/DSVActions';
import shortid from 'shortid';

const defaultAction = {
  type: '',
  payload: {}
};

const initialState = {
  rows: []
};

const DSV = (state = initialState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case DSVActions.setProperty:
      stateCopy = Object.assign({}, state);
      if (action.payload.property === null || typeof action.payload.property === 'undefined') {
        return stateCopy;
      }
      stateCopy.rows[action.payload.index].property = action.payload.property;
      return stateCopy;

    case DSVActions.addRow:
      stateCopy = Object.assign({}, state);
      stateCopy.rows.splice(action.payload.index + 1, 0, {
        property: '',
        uniqueId: shortid.generate()
      });
      return stateCopy;
    case DSVActions.deleteRow:
      stateCopy = Object.assign({}, state);
      stateCopy.rows.splice(action.payload.index, 1);
      if (!stateCopy.rows.length) {
        stateCopy.rows.push({
          property: '',
          uniqueId: shortid.generate()
        });
      }
      return stateCopy;
    case DSVActions.onReset:
      return initialState;
    case DSVActions.onUpdate:
      stateCopy = Object.assign({}, state);
      stateCopy.rows = action.payload.rows;
      return stateCopy;
    default:
      return state;
  }
};

const createDSVStore = (initialState = initialState) => {
  return createStore(
    combineReducers({DSV}),
    initialState
  );
};

export { createDSVStore };
