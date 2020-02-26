/*
 * Copyright © 2019 Cask Data, Inc.
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

export enum PluginTypes {
  BATCHSOURCE = 'batchsource',
  TRANSFORM = 'transform',
  BATCHAGGREGATOR = 'batchaggregator',
  SPLITTERTRANSFORM = 'splittertransform',
  BATCHSINK = 'batchsink',

  ALERTPUBLISHER = 'alertpublisher',
  ERRORTRANSFORM = 'errortransform',
  BATCHJOINER = 'batchjoiner',
  WINDOWER = 'windower',

  STREAMINGSOURCE = 'streamingsource',
  SPARKSINK = 'sparksink',
}

export interface IArtifactObj {
  name: string;
  version: string;
  scope: string;
}

export interface IProperties {
  [key: string]: any;
}

export interface IPluginObj {
  name: string;
  artifact: IArtifactObj;
  properties: IProperties;
  label: string;
}

export interface INode {
  name: string;
  type: PluginTypes;
  icon: string;
  plugin: IPluginObj;
  label: string;
}

export interface IConnection {
  from: string;
  to: string;
}
