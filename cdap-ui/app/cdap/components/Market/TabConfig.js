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
import AllTabContents from './AllTab';
import UsecaseTab from './UsecaseTab';
import T from 'i18n-react';



const TabConfig = {
  defaultTab: 1,
  defaultTabContent: <AllTabContents />,
  layout: 'vertical',
  tabs: [
    {
      id: 1,
      filter: '*',
      icon: 'icon-all',
      name: T.translate('features.Market.tabs.all'),
      content: <AllTabContents />
    },
    {
      id: 3,
      filter: 'usecase',
      icon: 'icon-usecases',
      name: T.translate('features.Market.tabs.useCases'),
      content: <UsecaseTab />
    },
    {
      id: 4,
      filter: 'pipeline',
      icon: 'icon-pipelines',
      name: T.translate('features.Market.tabs.pipelines'),
      content: <AllTabContents />
    },
    {
      id: 5,
      filter: 'example',
      icon: 'icon-app',
      name: T.translate('features.Market.tabs.examples'),
      content: <AllTabContents />
    },
    {
      id: 7,
      filter: 'hydrator-plugin',
      icon: 'icon-plug',
      name: T.translate('features.Market.tabs.plugins'),
      content: <AllTabContents />
    },
    {
      id: 6,
      filter: 'datapack',
      icon: 'icon-datapacks',
      name: T.translate('features.Market.tabs.datapacks'),
      content: <AllTabContents />
    },
    {
      id: 9,
      filter: '3rd-party-artifact',
      icon: 'icon-artifacts',
      name: T.translate('features.Market.tabs.artifacts'),
      content: <AllTabContents />
    },
    {
      id: 10,
      filter: 'EDW Offloading',
      icon: 'icon-database',
      name: T.translate('features.Market.tabs.edwOffload'),
      content: <AllTabContents />
    }
  ]
};

export default TabConfig;
