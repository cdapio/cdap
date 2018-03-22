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

export function getData(start, duration = 1440, namespaces = DashboardStore.getState().namespaces.namespacesPick) {
  enableLoading();

  let state = DashboardStore.getState().dashboard;

  if (!start) {
    start = moment().subtract(24, 'h').format('x');
    start = parseInt(start, 10);
  }

  let namespacesList = [...namespaces, getCurrentNamespace()];

  let params = {
    start,
    duration, // 24 hours in minutes
    namespace: namespacesList
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
          duration,
          namespacesPick: namespaces
        }
      });
    });
}

export function next() {
  let state = DashboardStore.getState().dashboard;

  let start = moment(state.startTime);

  if (state.duration === 1440) {
    start = start.add(12, 'h').format('x');
  } else {
    start = start.add(30, 'm').format('x');
  }

  start = parseInt(start, 10);

  getData(start, state.duration);
}

export function prev() {
  let state = DashboardStore.getState().dashboard;

  let start = moment(state.startTime);

  if (state.duration === 1440) {
    start = start.subtract(12, 'h').format('x');
  } else {
    start = start.subtract(30, 'm').format('x');
  }

  start = parseInt(start, 10);

  getData(start, state.duration);
}

export function setNamespacesPick(namespacesPick) {
  let state = DashboardStore.getState().dashboard;

  getData(state.startTime, state.duration, namespacesPick);
}
