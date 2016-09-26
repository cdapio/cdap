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
import GeneralInfoStep from 'components/CaskWizards/AddNamespace/GeneralInfoStep';
import MappingStep from 'components/CaskWizards/AddNamespace/MappingStep';
import SecurityStep from 'components/CaskWizards/AddNamespace/SecurityStep';
import PreferencesStep from 'components/CaskWizards/AddNamespace/PreferencesStep';
import T from 'i18n-react';

const AddNamespaceWizardConfig = {
  steps: [
    {
      id: 'general',
      shorttitle: T.translate('features.Wizard.Add-Namespace.Step1.ssd-label'),
      title: T.translate('features.Wizard.Add-Namespace.Step1.ssd-label'),
      description: T.translate('features.Wizard.Add-Namespace.Step1.sld-desc'),
      content: (<GeneralInfoStep />),
      requiredFields: ['name']
    },
    {
      id: 'mapping',
      shorttitle: T.translate('features.Wizard.Add-Namespace.Step2.ssd-label'),
      title: T.translate('features.Wizard.Add-Namespace.Step2.ssd-label'),
      description: T.translate('features.Wizard.Add-Namespace.Step2.sld-label'),
      content: (<MappingStep />)
    },
    {
      id: 'security',
      shorttitle: T.translate('features.Wizard.Add-Namespace.Step3.ssd-label'),
      title: T.translate('features.Wizard.Add-Namespace.Step3.ssd-label'),
      description: T.translate('features.Wizard.Add-Namespace.Step3.sld-label'),
      content: (<SecurityStep />)
    },
    {
      id: 'preferences',
      shorttitle: T.translate('features.Wizard.Add-Namespace.Step4.ssd-label'),
      title: T.translate('features.Wizard.Add-Namespace.Step4.ssd-label'),
      description: T.translate('features.Wizard.Add-Namespace.Step4.sld-label'),
      content: (<PreferencesStep />)
    }
  ]
};

export default AddNamespaceWizardConfig;
