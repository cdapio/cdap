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

const PREFIX = 'features.NUX';

const tour = new GuidedTour();

const steps: ITourStep[] = [
  {
    id: 'control-center',
    title: T.translate(`${PREFIX}.ControlCenter.title`).toString(),
    text: [
      T.translate(`${PREFIX}.ControlCenter.text`).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Control_Center_NUX.png" />',
    ],
    attachTo: '#navbar-control-center bottom',
  },
  {
    id: 'preparation',
    title: T.translate(`${PREFIX}.Preparation.title`).toString(),
    text: [
      T.translate(`${PREFIX}.Preparation.text`).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Dataprep_NUX.png" />',
    ],
    attachTo: '#navbar-preparation bottom',
  },
  {
    id: 'pipelines',
    title: T.translate(`${PREFIX}.Pipelines.title`).toString(),
    text: [
      T.translate(`${PREFIX}.Pipelines.text`).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Pipeline_NUX.png" />',
    ],
    attachTo: '#navbar-pipelines bottom',
  },
  {
    id: 'metadata',
    title: T.translate(`${PREFIX}.Metadata.title`).toString(),
    text: [
      T.translate(`${PREFIX}.Metadata.text`).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Metadata_NUX.png" />',
    ],
    attachTo: '#navbar-metadata bottom',
  },
  {
    id: 'hub',
    title: T.translate(`${PREFIX}.Hub.title`).toString(),
    text: [
      T.translate(`${PREFIX}.Hub.text`).toString(),
      '<img class="img-fluid" src="/cdap_assets/img/nux/Hub_NUX.png" />',
    ],
    attachTo: '#navbar-hub bottom',
  },
];

tour.addSteps(steps);

export default tour;
