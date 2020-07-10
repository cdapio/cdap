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

import { PrimaryButton, SecondaryButton, TextButton, DeleteIconButton, LoadingButton, MixedCaseButton } from './index';

storiesOf('Material Buttons', module)
  .addDecorator(withKnobs)
  .add(
    'Primary Button',
    withInfo({
      text: 'Primary button with Material UI'
    })(() => (
      <PrimaryButton
        aria-label={ text('Aria Label', 'aria label for button') }
        disabled={ boolean('Disabled', false) }>
        { text('Label', 'Primary action button') }
      </PrimaryButton>
    ))
  )
  .add(
    'Secondary Button',
    withInfo({
      text: 'Secondary button with Material UI'
    })(() => (
      <SecondaryButton
        aria-label={ text('Aria Label', 'aria label for button') }
        disabled={ boolean('Disabled', false) }>
        { text('Label', 'Secondary action button') }
      </SecondaryButton>
    ))
  )
  .add(
    'Text Button',
    withInfo({
      text: 'Text button with Material UI'
    })(() => (
      <TextButton
        aria-label={ text('Aria Label', 'aria label for button') }
        disabled={ boolean('Disabled', false) }>
        { text('Label', 'Text button') }
      </TextButton>
    ))
  )
  .add(
    'Delete Icon Button',
    withInfo({
      text: 'Delete icon button with Material UI'
    })(() => (
      <DeleteIconButton
        aria-label={ text('Aria Label', 'delete this entity') }
        disabled={ boolean('Disabled', false) }>
      </DeleteIconButton>
    ))
  )
  .add(
    'Loading Button',
    withInfo({
      text: 'Loading button with Material UI'
    })(() => (
      <LoadingButton
        aria-label={ text('Aria Label', 'aria label for button') }
        disabled={ boolean('Disabled', false) }
        loading={ boolean('Loading', true) }>
        { text('Label', 'Load my data') }
      </LoadingButton>
    ))
  )
  .add(
    'Mixed Case Button',
    withInfo({
      text: 'Override Material UI defaults'
    })(() => (
      <MixedCaseButton
        aria-label={ text('Aria Label', 'aria label for button') }
        disabled={ boolean('Disabled', false) }>
        { text('Label', 'cLiCk tHE ButToN Or ThE DOg gETs iT') }
      </MixedCaseButton>
    ))
  )