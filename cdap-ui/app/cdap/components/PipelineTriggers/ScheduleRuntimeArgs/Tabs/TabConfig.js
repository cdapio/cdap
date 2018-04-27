/*
 * Copyright © 2017 Cask Data, Inc.
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
import RuntimeArgsTab from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/RuntimeArgsTab';
import StagePropertiesTab from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/StagePropertiesTab';
import ComputeConfigTab from 'components/PipelineTriggers/ScheduleRuntimeArgs/Tabs/ComputeConfigTab';
import T from 'i18n-react';

const PREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs.Tabs';

const TabConfig = {
  tabs: [
    {
      id: 1,
      type: 'tab-group',
      name: T.translate(`${PREFIX}.PayloadConfig.title`),
      opened: true,
      subtabs: [
        {
          id: 1.1,
          name: T.translate(`${PREFIX}.RuntimeArgs.title`),
          content: (<RuntimeArgsTab />),
          default: true
        },
        {
          id: 1.2,
          name: T.translate(`${PREFIX}.StageProps.title`),
          content: (<StagePropertiesTab />)
        }
      ]
    },
    {
      id: 2,
      name: T.translate(`${PREFIX}.ComputeConfig.tabTitle`),
      content: (<ComputeConfigTab />)
    }
  ],
  layout: 'vertical',
  defaultTab: 1.1
};
export default TabConfig;
