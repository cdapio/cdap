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
import Typography from '@material-ui/core/Typography';

import { Modal, Wizard } from './index';

storiesOf('Material Modals and Dialogs', module)
  .addDecorator(withKnobs)
  .add(
    'Modal',
    withInfo({
      text: 'Basic modal'
    })(() => (
      <React.Fragment>
        <Typography component="h2">Material Dialog modal</Typography>
        <Modal onClose={ action('closed') } open={ boolean('Open', true) } />
      </React.Fragment>
    ))
  )
  .add(
    'Wizard',
    withInfo({
      text: 'Wizard UI concept'
    })(() => (
      <React.Fragment>
        <Typography component="h2">Material Dialog wizard concept</Typography>
        <Wizard onClose={ action('closed') } open={ boolean('Open', true) } />
      </React.Fragment>
    ))
  )