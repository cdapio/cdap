/*
 * Copyright © 2018 Cask Data, Inc.
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
import { storiesOf } from '@storybook/react';
import { withInfo } from '@storybook/addon-info';
import SortableStickyGrid from './index';

interface IHeaderType {
  property: string;
  label: string;
}
const GRID_HEADERS: IHeaderType[] = [
  {
    property: 'name',
    label: 'Name',
  },
  {
    property: 'email',
    label: 'Email Id',
  },
];

const entities = [
  {
    name: 'John',
    email: 'John@gmail.com',
  },
  {
    name: 'Peter',
    email: 'peter@gmail.com',
  },
  {
    name: 'Issac',
    email: 'issac@yahoo.com',
  },
];

storiesOf('Table', module)
  .add(
    'Simple Table',
    withInfo({
      text: 'Render the default table used across CDAP UI',
    })(() => <SortableStickyGrid entities={entities} gridHeaders={GRID_HEADERS} />)
  )
  .add(
    'Compact Table',
    withInfo({
      text: 'Render default table but compact one occupying lesser space',
    })(() => <SortableStickyGrid entities={entities} gridHeaders={GRID_HEADERS} size="small" />)
  );
