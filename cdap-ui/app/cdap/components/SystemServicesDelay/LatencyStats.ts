/*
 * Copyright © 2020 Cask Data, Inc.
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
import { IHealthCheckBindings, ILatencyData } from 'components/SystemServicesDelay/LatencyTypes';
import DataSource from 'services/datasource';
import { isNilOrEmpty } from 'services/helpers';
import flatten from 'lodash/flatten';

/**
 * Generates stats from requests across all datasources.
 *
 * - During each tick the function queries all datasources for active bindings
 * - For each bindings it maps through requests which has a completedRequestDuration & backendRequestTimeDuration
 * - If the request is in flight the duration is running one.
 * - Generate an array of objects with { id, x, y requestStartTime, networkDelay }
 *   - x: index for requests
 *   - y: complete duration of request
 *   - requestStartTime - start of request in nodejs server to backend
 *
 *
 * @param activeDataSources - Active datasources across all apis
 */
function GenerateStatsFromRequests(activeDataSources: DataSource[] = []): ILatencyData[] {
  const currentTime = Date.now();
  return flatten(
    activeDataSources
      .map((dataSource: DataSource) => {
        return dataSource.getBindingsForHealthCheck() as IHealthCheckBindings;
      })
      .filter((binding: IHealthCheckBindings) => {
        return !isNilOrEmpty(binding);
      })
      .map((binding: IHealthCheckBindings) => {
        return Object.keys(binding)
          .filter((k) => binding[k] && binding[k].requestTimeFromClient)
          .map((id) => {
            /**
             * For a request in flight we don't have `completedRequestDuration`.
             * So the duration of the request will be increasing until we get a response
             * from the backend. But once we got a response we stop calculating
             * the duration.
             */
            const {
              requestTimeFromClient: requestTime,
              completedRequestDuration,
              backendRequestTimeDuration,
              resource,
            } = binding[id];
            return {
              id,
              resource: { url: resource.url, method: resource.method },
              backendRequestTimeDuration,
              networkDelay: completedRequestDuration
                ? completedRequestDuration - backendRequestTimeDuration
                : 0,
              requestStartTime: requestTime,
            };
          });
      })
  )
    .sort((r1, r2) => r1.requestStartTime - r2.requestStartTime)
    .slice(-30);
}

export { GenerateStatsFromRequests };
