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

import * as React from 'react';
import AbstractWidget from './index';
import { withInfo } from '@storybook/addon-info';

export default {
  title: 'Widgets/Joins',
  component: AbstractWidget,
  decorators: [withInfo],
};

const inputSchema = [
  {
    name: 'Projection',
    schema:
      '{"type":"record","name":"etlSchemaBody","fields":[{"name":"haha","type":"string"},{"name":"hehe","type":"string"},{"name":"hohoho","type":"string"}]}',
  },
  {
    name: 'JavaScript',
    schema:
      '{"type":"record","name":"etlSchemaBody","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"string"},{"name":"field3","type":"string"}]}',
  },
];
const extraConfig = { inputSchema };

export const joinType = () => <AbstractWidget type="join-types" extraConfig={extraConfig} />;

export const sqlConditions = () => (
  <AbstractWidget type="sql-conditions" extraConfig={extraConfig} />
);

// TO DO: Fix i18n
export const sqlSelectFields = () => (
  <AbstractWidget type="sql-select-fields" extraConfig={extraConfig} />
);
