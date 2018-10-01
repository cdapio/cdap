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
import { action } from '@storybook/addon-actions';
import { withInfo } from '@storybook/addon-info';
import { withKnobs, boolean, text } from '@storybook/addon-knobs';
import BtnWithLoading from './index';

storiesOf('Button With Loading Icon', module)
  .addDecorator(withKnobs)
  .add('with text',
    withInfo({
      text: `
        Render button without loading icon
      `,
    })(() => (
      <BtnWithLoading
        onClick={action('clicked')}
        label={text('Label', "Hello Button")}
        loading={boolean('Loading', true)}
        disabled={boolean('Disabled', false)}
        className="btn btn-secondary"
        darker={boolean('Darker?', true)}
      />
  )))
  .add('with some emoji',
    withInfo({
      text: `
        Render button with loading icon
      `,
    })(() => (
      <BtnWithLoading
        onClick={action('clicked')}
        label={text('Label', "👍 💯")}
        loading={boolean('Loading', true)}
        disabled={boolean('Disabled', true)}
        className="btn btn-primary"
      />
  )));
