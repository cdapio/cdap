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

import {createStore} from 'redux';
import {defaultAction, composeEnhancers} from 'services/helpers';

const DEFAULT_METRICS_STATE = {
  metrics: [],
  logsMetrics: {},
  // FIXME: There are probably better places to store these info, but adding them here
  // for now as the old metrics store had these info. Can come back and move these later
  metricsTabActive: false,
  portsToShow: [],
};

const PipelineMetricsActions = {
  SET_METRICS: 'SET_METRICS',
  SET_LOGS_METRICS: 'SET_LOGS_METRICS',
  SET_ACTIVE_TAB: 'SET_ACTIVE_TAB',
  RESET: 'RESET'
};

const metrics = (state = DEFAULT_METRICS_STATE, action = defaultAction) => {
  switch (action.type) {
    case PipelineMetricsActions.SET_METRICS:
      return {
        ...state,
        metrics: action.payload.metrics
      };
    case PipelineMetricsActions.SET_LOGS_METRICS:
      return {
        ...state,
        logsMetrics: action.payload.logsMetrics
      };
    case PipelineMetricsActions.SET_ACTIVE_TAB:
      return {
        ...state,
        metricsTabActive: action.payload.metricsTabActive,
        portsToShow: action.payload.portsToShow ? [action.payload.portsToShow] : state.portsToShow
      };
    case PipelineMetricsActions.RESET:
      return DEFAULT_METRICS_STATE;
  }
};

const PipelineMetricsStore = createStore(
  metrics,
  DEFAULT_METRICS_STATE,
  composeEnhancers('PipelineMetricsStore')()
);

export default PipelineMetricsStore;
export {PipelineMetricsActions};
