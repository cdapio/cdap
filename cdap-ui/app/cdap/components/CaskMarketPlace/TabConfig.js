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

import AllTabContents from './AllTabContents';

const TabConfig = {
  defaultTab: 1,
  layout: 'vertical',
  tabs: [
    {
      id: 1,
      name: 'All',
      content: <AllTabContents />
    },
    {
      id: 2,
      name: 'Examples',
      content: 'Examples Tab Content'
    },
    {
      id: 3,
      name: 'Use Cases',
      content: 'Use cases Tab Content'
    },
    {
      id: 4,
      name: 'Pipeline',
      content: 'Pipeline Tab Content'
    },
    {
      id: 5,
      name: 'Applications',
      content: 'Applications Tab Content'
    },
    {
      id: 6,
      name: 'Datasets',
      content: 'Datasets Tab Content'
    },
    {
      id: 7,
      name: 'Plugins',
      content: 'Plugins Tab Content'
    },
    {
      id: 8,
      name: 'Dashboards',
      content: 'Dashboards Tab Content'
    },
    {
      id: 9,
      name: 'Artifacts',
      content: 'Artifacts Tab Content'
    }
  ]
};

export default TabConfig;
