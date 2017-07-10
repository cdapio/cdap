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
const PIPELINESSUMMARYACTIONS = {
  SETRUNS: 'SETRUNS',
  SETMETRICS: 'SETMETRICS',
  SETLOADING: 'SETLOADING'
};
const defaultRunsSummary = {
  runs: [],
  loading: false
};

const pipelinerunssummary = (state = defaultRunsSummary, action = defaultAction) => {
  switch (action.type) {
    case PIPELINESSUMMARYACTIONS.SETRUNS:
      return Object.assign({}, state, {
        runs: action.payload.runs || [],
        loading: false
      });
    case PIPELINESSUMMARYACTIONS.SETLOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading
      });
    default:
      return state;
  }
};

const PipelineSummaryStore = createStore(
  combineReducers({
    pipelinerunssummary
  }),
  {
    pipelinerunssummary: defaultRunsSummary
  }
);

export {PIPELINESSUMMARYACTIONS};
export default PipelineSummaryStore;
