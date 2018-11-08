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

import GuidedTour, { ITourStep } from 'services/GuidedTour';
import T from 'i18n-react';
import { Theme } from 'services/ThemeHelper';

const PREFIX = 'features.NUX';
const featureNames = Theme.featureNames;

const tour = new GuidedTour();

const steps: ITourStep[] = [
  {
    id: 'control-center',
    title: featureNames.controlCenter,
    text: [
      T.translate(`${PREFIX}.ControlCenter.text`, {
        featureName: featureNames.controlCenter,
      }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Control_Center_NUX.png" />',
    ],
    attachTo: '#navbar-control-center bottom',
    shouldFocus: true,
  },
  {
    id: 'pipelines',
    title: featureNames.pipelines,
    text: [
      T.translate(`${PREFIX}.Pipelines.text`, { featureName: featureNames.pipelines }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Pipeline_NUX.png" />',
    ],
    attachTo: '#navbar-pipelines bottom',
    shouldFocus: true,
  },
  {
    id: 'preparation',
    title: featureNames.dataPrep,
    text: [
      T.translate(`${PREFIX}.Preparation.text`, { featureName: featureNames.dataPrep }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Dataprep_NUX.png" />',
    ],
    attachTo: '#navbar-preparation bottom',
    shouldFocus: true,
  },
  {
    id: 'metadata',
    title: featureNames.metadata,
    text: [
      T.translate(`${PREFIX}.Metadata.text`, { featureName: featureNames.metadata }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Metadata_NUX.png" />',
    ],
    attachTo: '#navbar-metadata bottom',
    shouldFocus: true,
  },
  {
    id: 'hub',
    title: featureNames.hub,
    text: [
      T.translate(`${PREFIX}.Hub.text`, {
        productName: Theme.productName,
        featureName: featureNames.hub,
      }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Hub_NUX.png" />',
    ],
    attachTo: '#navbar-hub bottom',
    shouldFocus: true,
  },
];

tour.addSteps(steps);

export default tour;
