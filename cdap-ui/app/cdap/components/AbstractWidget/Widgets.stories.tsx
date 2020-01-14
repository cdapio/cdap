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
import { storiesOf } from '@storybook/react';
import { withInfo } from '@storybook/addon-info';
import { action } from '@storybook/addon-actions';
import AbstractWidget from './index';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Select from 'components/AbstractWidget/FormInputs/Select';

const options = [
  { value: 'choice A', label: 'choice A' },
  { value: 'choice B', label: 'choice B' },
];

const widgetProps = { options };

storiesOf('Widget', module).add(
  'Select',
  withInfo({
    text: 'Render a select dropdown widget',
  })(() => (
    <AbstractWidget
      value={'choice A'}
      type="select"
      widgetProps={widgetProps}
      onChange={action('onChange')}
    />
  ))
);
