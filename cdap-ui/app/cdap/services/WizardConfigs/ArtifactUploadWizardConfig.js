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
import UploadStep from 'components/CaskWizards/ArtifactUpload/UploadStep';
import ConfigureStep from 'components/CaskWizards/ArtifactUpload/ConfigureStep';

import T from 'i18n-react';
let commonSteps = [
  {
    id: 'upload',
    shorttitle: T.translate('features.Wizard.ArtifactUpload.Step1.shorttitle'),
    title: T.translate('features.Wizard.ArtifactUpload.Step1.title'),
    description: T.translate('features.Wizard.ArtifactUpload.Step1.description'),
    content: (<UploadStep />),
    requiredFields: ['file']
  },
  {
    id: 'configuration',
    shorttitle: T.translate('features.Wizard.ArtifactUpload.Step2.shorttitle'),
    title: T.translate('features.Wizard.ArtifactUpload.Step2.title'),
    description: T.translate('features.Wizard.ArtifactUpload.Step2.description'),
    content: (<ConfigureStep />),
    requiredFields: ['name', 'type', 'parentArtifact', 'classname']
  }
];

const ArtifactUploadWizardConfig = {
  steps: commonSteps,
  footertitle: T.translate('features.Wizard.ArtifactUpload.footertitle')
};


export default ArtifactUploadWizardConfig;
