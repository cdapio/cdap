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
import T from 'i18n-react';

const TabConfig = {
  defaultTab: 1,
  layout: 'vertical',
  tabs: [
    {
      id: 1,
      name: T.translate('features.Market.tabs.all'),
      content: <AllTabContents />
    },
    {
      id: 2,
      name: T.translate('features.Market.tabs.examples'),
      content: <AllTabContents />
    },
    {
      id: 3,
      name: T.translate('features.Market.tabs.useCases'),
      content: <AllTabContents />
    },
    {
      id: 4,
      name: T.translate('features.Market.tabs.pipelines'),
      content: <AllTabContents />
    },
    {
      id: 5,
      name: T.translate('features.Market.tabs.applications'),
      content: <AllTabContents />
    },
    {
      id: 6,
      name: T.translate('features.Market.tabs.datasets'),
      content: <AllTabContents />
    },
    {
      id: 7,
      name: T.translate('features.Market.tabs.plugins'),
      content: <AllTabContents />
    },
    {
      id: 8,
      name: T.translate('features.Market.tabs.dashboards'),
      content: <AllTabContents />
    },
    {
      id: 9,
      name: T.translate('features.Market.tabs.artifacts'),
      content: <AllTabContents />
    }
  ]
};

export default TabConfig;
