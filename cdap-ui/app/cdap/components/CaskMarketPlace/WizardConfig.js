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

const WizardConfig = {
  defaultTab: 0,
  mode: 'vertical',
  tabs: [
    {
      title: 'All',
      content: <AllTabContents />
    },
    {
      title: 'Examples',
      content: 'Examples Tab Content'
    },
    {
      title: 'Use Cases',
      content: 'Use cases Tab Content'
    },
    {
      title: 'Pipeline',
      content: 'Pipeline Tab Content'
    },
    {
      title: 'Applications',
      content: 'Applications Tab Content'
    },
    {
      title: 'Datasets',
      content: 'Datasets Tab Content'
    },
    {
      title: 'Plugins',
      content: 'Plugins Tab Content'
    },
    {
      title: 'Dashboards',
      content: 'Dashboards Tab Content'
    },
    {
      title: 'Artifacts',
      content: 'Artifacts Tab Content'
    }
  ]
};

export {WizardConfig};
