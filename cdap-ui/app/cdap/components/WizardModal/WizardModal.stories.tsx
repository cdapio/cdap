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
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';

import InformationalWizardConfig from 'services/WizardConfigs/InformationalWizardConfig';
import InformationalWizardStore from 'services/WizardStores/Informational/InformationalStore';
import InformationalActions from 'services/WizardStores/Informational/InformationalActions';


storiesOf('WizardModal', module)
  .addDecorator(withKnobs)
  .add(
    'Default Wizard',
    withInfo({
      text: 'Demo wizard with a few steps'
    })(() => {

      const state = {
        step0: {
          __complete: false,
          __skipped: false,
          __error: false
        },
        step1: {
          __complete: false,
          __skipped: false,
          __error: false
        },
        step2: {
          __complete: true,
          __skipped: false,
          __error: false
        }
      };

      const store = {
        getState: () => state,
        dispatch: action('dispatch'),
        subscribe: action('subscribe'),
        replaceReducer: action('replaceReducer')
      };

      const config = {
        steps: [{
          id: 'step0',
          title: 'First Step',
          shorttitle: 'First Step',
          description: 'Description of First Step',
          content: <div className="form-horizontal">Content of First Step</div>,
        }, {
          id: 'step1',
          title: 'Second Step',
          shorttitle: 'Second Step',
          description: 'Description of Second Step',
          content: <div className="form-horizontal">Content of Second Step</div>,
        }, {
          id: 'step2',
          title: 'Third Step',
          shorttitle: 'Third Step',
          description: 'Description of Third Step',
          content: <div className="form-horizontal">Content of Third Step</div>,
        }]
      };

      return <WizardModal
        title="Default Wizard"
        isOpen={boolean('Open', true)}
        toggle={action('toggled')}
      >
        <Wizard
          wizardConfig={config}
          wizardType="DemoWizard"
          onSubmit={action('submitted')}
          onClose={action('closed')}
          store={store}
        />
      </WizardModal>
    })
  )