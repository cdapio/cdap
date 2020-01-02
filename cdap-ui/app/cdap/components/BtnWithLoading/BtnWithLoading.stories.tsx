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

/**
 * FIXME: Once we have styled components or material ui we should have our own
 * Button component with everything configurable. This would help us have support for a11y.
 */
storiesOf('Buttons', module)
  .addDecorator(withKnobs)
  .add(
    'Loading button with text',
    withInfo({
      text: 'Render button without loading icon',
    })(() => (
      <BtnWithLoading
        onClick={action('clicked')}
        label={text('Label', 'Hello Button')}
        loading={boolean('Loading', true)}
        disabled={boolean('Disabled', false)}
        className="btn btn-secondary"
        darker={boolean('Darker?', true)}
      />
    ))
  )
  .add(
    'Primary Button',
    withInfo({
      text: 'Render primary button',
    })(() => <button className="btn btn-primary">{text('Label', 'Primary Action Button')}</button>)
  )
  .add(
    'Secondary Button',
    withInfo({
      text: 'Render secondary button',
    })(() => (
      <button className="btn btn-secondary">{text('Label', 'Secondary Action Button')}</button>
    ))
  )
  .add(
    'Link Button',
    withInfo({
      text: 'Render success button',
    })(() => (
      <button className="btn btn-link">{text('Label', 'Another Secondary Action Button')}</button>
    ))
  )
  .add(
    'Success Button',
    withInfo({
      text: 'Render success button',
    })(() => <button className="btn btn-success">{text('Label', 'Success Button')}</button>)
  );
