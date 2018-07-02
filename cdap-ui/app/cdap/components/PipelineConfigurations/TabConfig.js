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

import React from 'react';
import PipelineConfigTabContent from 'components/PipelineConfigurations/ConfigurationsContent/PipelineConfigTabContent';
import EngineConfigTabContent from 'components/PipelineConfigurations/ConfigurationsContent/EngineConfigTabContent';
import ResourcesTabContent from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent';
import AlertsTabContent from 'components/PipelineConfigurations/ConfigurationsContent/AlertsTabContent';
import ComputeTabContent from 'components/PipelineConfigurations/ConfigurationsContent/ComputeTabContent';

const TabConfig = {
  tabs: [
    {
      id: 1,
      name: 'Compute Config',
      content: (<ComputeTabContent />),
      contentClassName: 'pipeline-configurations-body',
      paneClassName: 'configuration-content'
    },
    {
      id: 2,
      name: 'Pipeline Config',
      content: (<PipelineConfigTabContent />),
      contentClassName: 'pipeline-configurations-body',
      paneClassName: 'configuration-content'
    },
    {
      id: 3,
      name: 'Engine Config',
      content: (<EngineConfigTabContent />),
      contentClassName: 'pipeline-configurations-body',
      paneClassName: 'configuration-content'
    },
    {
      id: 4,
      name: 'Resources',
      content: (<ResourcesTabContent />),
      contentClassName: 'pipeline-configurations-body',
      paneClassName: 'configuration-content'
    },
    {
      id: 5,
      name: 'Pipeline Alert',
      content: (<AlertsTabContent />),
      contentClassName: 'pipeline-configurations-body',
      paneClassName: 'configuration-content'
    }
  ],
  layout: 'vertical',
  defaultTab: 1
};
export default TabConfig;
