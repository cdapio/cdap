/*
 * Copyright © 2016 Cask Data, Inc.
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

import React from 'react';
import GeneralInfoStep from 'components/CaskWizards/StreamCreate/GeneralInfoStep';
import SchemaStep from 'components/CaskWizards/StreamCreate/SchemaStep';
import ThresholdStep from 'components/CaskWizards/StreamCreate/ThresholdStep';

import T from 'i18n-react';
let commonSteps = [
  {
    id: 'general',
    shorttitle: T.translate('features.Wizard.StreamCreate.Step1.shorttitle'),
    title: T.translate('features.Wizard.StreamCreate.Step1.title'),
    description: T.translate('features.Wizard.StreamCreate.Step1.description'),
    content: (<GeneralInfoStep />),
    requiredFields: ['name']
  },
  {
    id: 'schema',
    shorttitle: T.translate('features.Wizard.StreamCreate.Step2.shorttitle'),
    title: T.translate('features.Wizard.StreamCreate.Step2.title'),
    description: T.translate('features.Wizard.StreamCreate.Step2.description'),
    content: (<SchemaStep />)
  },
  {
    id: 'threshold',
    shorttitle: T.translate('features.Wizard.StreamCreate.Step3.shorttitle'),
    title: T.translate('features.Wizard.StreamCreate.Step3.title'),
    description: T.translate('features.Wizard.StreamCreate.Step3.description'),
    content: (<ThresholdStep />)
  }
];

const CreateStreamWizardConfig = {
  steps: commonSteps
};


export default CreateStreamWizardConfig;
