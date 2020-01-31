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
import ee from 'event-emitter';

const PREFIX = 'features.NUX';
const featureNames = Theme.featureNames;

const tour = new GuidedTour();
const popperOptions = {
  placement: 'right',
  modifiers: {
    preventOverflow: {
      priority: ['right', 'bottom'],
      escapeWithReference: false,
      boundariesElement: 'viewport',
    },
    offset: {
      enabled: true,
      offset: 135,
    },
  },
};
const steps: ITourStep[] = [
  {
    id: 'pipelines',
    title: `${featureNames.pipelines} ${featureNames.pipelineStudio}`,
    text: [
      T.translate(`${PREFIX}.Pipelines.text`, {
        featureName: featureNames.pipelineStudio,
      }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Pipeline_studio_NUX.png" />',
    ],
    attachTo: '#navbar-pipeline-studio > div right',
    popperOptions,
    shouldFocus: true,
  },
  {
    id: 'preparation',
    title: featureNames.dataPrep,
    text: [
      T.translate(`${PREFIX}.Preparation.text`, { featureName: featureNames.dataPrep }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Dataprep_NUX.png" />',
    ],
    attachTo: '#navbar-preparation > div right',
    popperOptions,
    shouldFocus: true,
  },
  {
    id: 'metadata',
    title: featureNames.metadata,
    text: [
      T.translate(`${PREFIX}.Metadata.text`, { featureName: featureNames.metadata }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Metadata_NUX.png" />',
    ],
    attachTo: '#navbar-metadata > div right',
    popperOptions,
    shouldFocus: true,
  },
  {
    id: 'control-center',
    title: featureNames.controlCenter,
    text: [
      T.translate(`${PREFIX}.ControlCenter.text`, {
        featureName: featureNames.controlCenter,
      }).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Control_Center_NUX.png" />',
    ],
    attachTo: '#navbar-control-center > div right',
    popperOptions: {
      placement: 'right',
      modifiers: {
        preventOverflow: {
          escapeWithReference: false,
          boundariesElement: 'viewport',
          padding: 10,
        },
      },
    },
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

const eventEmitter = ee(ee);

function hideAppDrawer() {
  eventEmitter.emit('NUX-TOUR-END');
}

tour.on('cancel', hideAppDrawer);
tour.on('complete', hideAppDrawer);

export default tour;
