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
import T from 'i18n-react';
import ViewDataStep from 'components/CaskWizards/UploadData/ViewDataStep';
import SelectDestination from 'components/CaskWizards/UploadData/SelectDestination';

const UploadDataWizardConfig = {
  steps: [
    {
      id: 'viewdata',
      shorttitle: T.translate('features.Wizard.UploadData.Step1.shorttitle'),
      title: T.translate('features.Wizard.UploadData.Step1.title'),
      description: T.translate('features.Wizard.UploadData.Step1.description'),
      content: (<ViewDataStep />),
    },
    {
      id: 'selectdestination',
      shorttitle: T.translate('features.Wizard.UploadData.Step2.shorttitle'),
      title: T.translate('features.Wizard.UploadData.Step2.title'),
      description: T.translate('features.Wizard.UploadData.Step2.description'),
      content: (<SelectDestination />),
      requiredFields: ['name', 'type']
    }
  ]
};

export default UploadDataWizardConfig;
