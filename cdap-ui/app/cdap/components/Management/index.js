/*
 * Copyright © 2016 Cask Data, Inc.
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

import React from 'react';
import ManagementView from '../ManagementView';

var dummyData = {
  version: "3.4",
  uptime: {
    duration: 0.2,
    unit: 'hr'
  },
  services: [
    {
      name: 'App Fabric',
      status: 'Green'
    },
    {
      name: 'Explore',
      status: 'Green'
    },
    {
      name: 'Metadata',
      status: 'Green'
    },
    {
      name: 'Metrics Processor',
      status: 'Green'
    },
    {
      name: 'Tephra Transaction',
      status: 'Red'
    },
    {
      name: 'Dataset Executor',
      status: 'Green'
    },
    {
      name: 'Log Saver',
      status: 'Green'
    },
    {
      name: 'Streams',
      status: 'Yellow'
    }
  ]
};

export default function Management() {
  return (
    <div className="management">
      <ManagementView data={dummyData}/>
    </div>
  );
}
