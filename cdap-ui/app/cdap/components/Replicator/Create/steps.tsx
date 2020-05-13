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

import NameDescription from 'components/Replicator/Create/Content/NameDescription';
import SourceConfig from 'components/Replicator/Create/Content/SourceConfig';
import SelectTables from 'components/Replicator/Create/Content/SelectTables';
import TargetConfig from 'components/Replicator/Create/Content/TargetConfig';
import Assessment from 'components/Replicator/Create/Content/Assessment';
import Advanced from 'components/Replicator/Create/Content/Advanced';
import Summary from 'components/Replicator/Create/Content/Summary';

export const STEPS = [
  {
    label: 'Name replicator',
    component: NameDescription,
  },
  {
    label: 'Configure source',
    component: SourceConfig,
  },
  {
    label: 'Select tables',
    component: SelectTables,
  },
  {
    label: 'Configure target',
    component: TargetConfig,
  },
  {
    label: 'Advanced',
    component: Advanced,
  },
  {
    label: 'Assessment',
    component: Assessment,
  },
  {
    label: 'Review',
    component: Summary,
  },
];
