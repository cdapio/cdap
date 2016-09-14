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
const CreateStreamWizardConfig = {
  steps: [
    {
      id: 'general',
      shorttitle: 'General Information',
      title: 'General',
      description: 'Provide information about Stream you want to create.',
      content: (<GeneralInfoStep />),
      requiredFields: ['name'],
      onSubmitGoTo: 'uploaddata'
    },
    {
      id: 'schema',
      shorttitle: 'Setup Format and Schema',
      title: 'Set Format and Schema',
      description: 'Setting format and schema allows you to perform schema-on-read.',
      content: (<SchemaStep />),
      onSubmitGoTo: 'uploaddata'
    },
    {
      id: 'threshold',
      shorttitle: 'Trigger Setup',
      title: 'Setup Trigger',
      description: 'Setting up Trigger configures system to notify systems observing to start processing.',
      content: (<ThresholdStep />),
      onSubmitGoTo: 'uploaddata'
    },
    {
      id: 'uploaddata',
      shorttitle: 'Upload Data',
      title: 'Upload Data',
      description: 'Upload data to the stream you just created.',
      content: <h1> Upload Step </h1>
    }
  ]
};

export default CreateStreamWizardConfig;
