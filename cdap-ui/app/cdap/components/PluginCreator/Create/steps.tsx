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

import NameDescription from 'components/PluginCreator/Create/Content/NameDescription';
import SourceConfig from 'components/PluginCreator/Create/Content/SourceConfig';
import SelectTables from 'components/PluginCreator/Create/Content/SelectTables';
import TargetConfig from 'components/PluginCreator/Create/Content/TargetConfig';
import Assessment from 'components/PluginCreator/Create/Content/Assessment';
import Advanced from 'components/PluginCreator/Create/Content/Advanced';
import Summary from 'components/PluginCreator/Create/Content/Summary';

export const STEPS = [
  /*{
    label: 'Name JSON file',
    component: NameDescription,
  },*/
  {
    label: 'Configure groups',
    component: SourceConfig,
  },
  /*{
    label: 'Add Widgets',
    component: SelectTables,
  },*/
];
