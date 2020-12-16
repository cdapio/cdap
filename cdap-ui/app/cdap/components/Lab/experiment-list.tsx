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

export default [
  {
    name: 'CDAP Common',
    description: `This is a common flag that developers can use to hide or show features. The flag is stored in browser's local storage with experiment ID as the name.`,
    experimentId: 'cdap-common-experiment',
    screenshot: null,
    enabled: false,
  },
  {
    name: 'Virtual scroll demo',
    description: 'This is a demo of the virtual scroll component.',
    experimentId: 'virtual-scroll-demo',
    screenshot: null,
    enabled: false,
  },
  {
    name: 'Data Ingestion',
    description: `Easily transfer data between a source and a sink.`,
    experimentId: 'data-ingestion',
    screenshot: '/cdap_assets/img/ingest-tile.svg',
    enabled: false,
  },
  {
    name: 'System Delay Notification',
    description: `Persistent notification indicating if there is a delay in getting responses from backend. A health check is done every 12 seconds.`,
    experimentId: 'system-delay-notification',
    screenshot: null,
    enabled: false,
    force: true,
    showValue: true,
    valueType: 'number',
    valueLabel: 'Max delay in seconds',
  },
  {
    name: 'Schema Editor',
    description: 'New SchemaEditor. Includes complete rewrite in React + perf improvements',
    experimentId: 'schema-editor',
    screenshot: null,
    enabled: true,
  },
];
