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

const tour = new GuidedTour();

const steps: ITourStep[] = [
  {
    id: 'welcome',
    title: 'Welcome to CDAP',
    text: [
      'CDAP is an open source framework that simplifies data application development,' +
      ' data integration and data management.',
      'Take a short tour to discover all that you can do.',
    ],
    attachTo: '.brand-section bottom',
    shouldFocus: true,
  },
  {
    id: 'control-center',
    title: 'Control Center',
    text: 'Control Center allows you to create, manage, operate and monitor datasets and applications. ',
    attachTo: '#navbar-control-center bottom',
    shouldFocus: true,
  },
  {
    id: 'preparation',
    title: 'Preparation',
    text: 'Preparation allows you to easily connect to a variety of data sources and cleanse data' +
      ' using point and click interactions. Once you are satisfied, you can operationalize your' +
      ' transformations in a pipeline. ',
    attachTo: '#navbar-preparation bottom',
    shouldFocus: true,
  },
];

tour.addSteps(steps);

export default tour;
