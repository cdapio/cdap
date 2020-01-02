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
import Alert from './index';

storiesOf('Alert', module)
  .add(
    'Success',
    withInfo({
      text: 'Render a success alert',
    })(() => (
      <Alert
        showAlert={true}
        message="Marty! You are back."
        onClose={action('closed')}
        type="success"
      />
    ))
  )
  .add(
    'Error',
    withInfo({
      text: 'Render a error alert',
    })(() => (
      <Alert
        showAlert={true}
        message="Great scott! How do you get 1.21GW of electricty!"
        onClose={action('closed')}
        type="error"
      />
    ))
  )
  .add(
    'Info',
    withInfo({
      text: 'Render an information alert',
    })(() => (
      <Alert
        showAlert={true}
        message="Hey! Hey! Hey! Hey! All I want is a pepsi"
        onClose={action('closed')}
        type="info"
      />
    ))
  );
