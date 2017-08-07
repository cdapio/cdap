/*
 * Copyright © 2017 Cask Data, Inc.
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
import RulebookUploadStep from 'components/RulesEngineHome/ImportRulebookWizard/RulebookUploadStep';

let commonSteps = [
  {
    id: 'upload',
    shorttitle: 'Import Rulebook',
    title: 'Upload Rulebook',
    description: 'Upload your Rulebook file',
    content: (<RulebookUploadStep />),
    requiredFields: ['file']
  }
];

const RulebookUploadWizardConfig = {
  steps: commonSteps,
  footertitle: 'Failed to Upload Rulebook'
};


export default RulebookUploadWizardConfig;
