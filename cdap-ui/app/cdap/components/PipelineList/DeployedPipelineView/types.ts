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

export interface IApplicationRecord {
  type: string;
  name: string;
  version: string;
  description: string;
  artifact: IArtifactSummary;
  ownerPrincipal?: string;
  applicationDetail: IApplicationDetail;
}

export interface IApplicationDetail {
  name: string;
  appVersion: string;
  description: string;
  configuration: string;
  programs: IProgramRecord[];
  artifact: IArtifactSummary;
  ownerPrincipal?: string;
}

export interface IProgramRecord {
  type: string;
  app: string;
  name: string;
  description: string;
  runs: IRunRecord[];
}

export interface IWorkflow extends IProgramRecord {
  schedules: IScheduleDetail[];
}

export interface IRunRecord {
  runid: string;
  starting: string;
  start: string;
  end: string;
  status: string;
  profileId?: string;
}

export interface IArtifactSummary {
  name: string;
  version: string;
  scope: string;
}

export interface IScheduleDetail {
  namespace: string;
  application: string;
  applicationVersion: string;
  name: string;
  description: string;
  timeoutMillis: string;
  status: string;
  nextRuntimes: string[];
}
