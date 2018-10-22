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
import T from 'i18n-react';
import Deploy from 'components/CaskWizards/OneStepDeploy/Deploy';

const OneStepDeployPluginConfig = {
  steps: [
    {
      id: 'one_step_deploy_plugin',
      shorttitle: T.translate('features.Wizard.OneStepDeploy.Step1.shorttitle', {
        entityType: 'Plugin',
      }),
      title: T.translate('features.Wizard.OneStepDeploy.Step1.title'),
      description: T.translate('features.Wizard.OneStepDeploy.Step1.description', {
        entityType: 'Plugin',
      }),
      content: <Deploy />,
    },
  ],
};

export default OneStepDeployPluginConfig;
