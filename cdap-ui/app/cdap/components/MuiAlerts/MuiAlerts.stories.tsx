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
import { withKnobs, boolean, text, number } from '@storybook/addon-knobs';
import { PrimaryButton } from 'components/MuiButtons';

import { SuccessAlert, ErrorAlert, InfoAlert, NotificationPermission } from './index';

storiesOf('Material Alerts', module)
  .addDecorator(withKnobs)
  .add(
    'Success Alert',
    withInfo({
      text: 'Success alert with snackbar'
    })(() => (
      <SuccessAlert
        duration={ number('Duration', 3000) }
        onClose={ action('closed') }
        open={ boolean('Open', true) }>
        { text('Label', "Marty, you're back!") }
      </SuccessAlert>
    ))
  )
  .add(
    'Error Alert',
    withInfo({
      text: 'Error alert with snackbar'
    })(() => (
      <ErrorAlert
        duration={ number('Duration', 3000) }
        onClose={ action('closed') }
        open={ boolean('Open', true) }>
        { text('Label', "Great scott! How do you get 1.21GW of electricty!") }
      </ErrorAlert>
    ))
  )
  .add(
    'Info Alert',
    withInfo({
      text: 'Info alert with snackbar'
    })(() => (
      <InfoAlert
        duration={ number('Duration', 3000) }
        onClose={ action('closed') }
        open={ boolean('Open', true) }>
        { text('Label', "Hey! Hey! Hey! Hey! All I want is a pepsi") }
      </InfoAlert>
    ))
  )
  .add(
    'System Notification',
    withInfo({
      text: 'Enable and use the system notification'
    })(() => {
      const alertTitle = text('Title', 'System notification!');
      const alertBody = text('Body', 'Lorem ipsum blah blah');
      return <React.Fragment>
        <NotificationPermission />
        <div>
          <PrimaryButton
            onClick={
              () => { new Notification(alertTitle, {
                body: alertBody,
                icon: '/img/favicon.png'
              }) }
            }>
            Show notification
          </PrimaryButton>
        </div>
      </React.Fragment>
    })
  );