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

import MyOperationsApi from 'api/operations';
import {getCurrentNamespace} from 'services/NamespaceStore';
import moment from 'moment';
import {parseDashboardData} from 'components/OpsDashboard/RunsGraph/DataParser';
import DashboardStore, {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';

export function enableLoading() {
  DashboardStore.dispatch({
    type: DashboardActions.enableLoading
  });
}

export function getData() {
  enableLoading();

  let state = DashboardStore.getState().dashboard;

  let start = moment().subtract(24, 'h').format('x'),
      duration = 1440;

  start = parseInt(start, 10);

  let params = {
    start,
    duration, // 24 hours in minutes
    namespace: getCurrentNamespace()
  };

  MyOperationsApi.getDashboard(params)
    .subscribe((res) => {
      let {
        pipelineCount,
        customAppCount,
        data
      } = parseDashboardData(res, start, duration, state.pipeline, state.customApp);

      DashboardStore.dispatch({
        type: DashboardActions.setData,
        payload: {
          rawData: res,
          data,
          pipelineCount,
          customAppCount,
          startTime: start,
          duration
        }
      });
    });
}
