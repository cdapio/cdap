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

import BasicPluginInfo from 'components/PluginJSONCreator/Create/Content/BasicPluginInfo';
import ConfigurationGroupsCollection from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection';
import Filters from 'components/PluginJSONCreator/Create/Content/Filters';
import Outputs from 'components/PluginJSONCreator/Create/Content/Outputs';

export const STEPS = [
  {
    label: 'Basic Plugin Information',
    component: BasicPluginInfo,
  },
  {
    label: 'Configuration Groups',
    component: ConfigurationGroupsCollection,
  },
  {
    label: 'Output',
    component: Outputs,
  },
  {
    label: 'Filters',
    component: Filters,
  },
];
