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

import { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import DataSource from 'services/datasource';
export interface IBindingRequestInfo {
  requestTimeFromClient: number;
  backendRequestTimeDuration: number;
  completedRequestDuration?: number;
}
export interface IHealthCheckBindings {
  [key: string]: IBindingRequestInfo;
}

export interface ISystemDelayProps extends WithStyles<StyleRules> {
  showDelay: boolean;
  activeDataSources: DataSource[];
}

export interface IChartDataType {
  x: number;
  y: number;
  id: string;
}

export interface IChartStatType {
  x: number;
  y: number;
  networkDelay: number;
  requestStartTime: number;
  id: string;
}

export interface ILatencyChildProps {
  requestDelayMap: Record<number, IBindingRequestInfo>;
}
