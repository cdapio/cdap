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

import T from 'i18n-react';

const CATEGORY_MAP = {
  usecase: {
    icon: 'icon-usecases',
    displayName: T.translate('features.Market.tabs.useCases'),
  },
  pipeline: {
    icon: 'icon-pipelines',
    displayName: T.translate('features.Market.tabs.pipelines'),
  },
  example: {
    icon: 'icon-app',
    displayName: T.translate('features.Market.tabs.examples'),
  },
  'hydrator-plugin': {
    icon: 'icon-plug',
    displayName: T.translate('features.Market.tabs.plugins'),
  },
  '3rd-party-artifact': {
    icon: 'icon-artifacts',
    displayName: T.translate('features.Market.tabs.artifacts'),
  },
  'EDW Offloading': {
    icon: 'icon-database',
    displayName: T.translate('features.Market.tabs.edwOffload'),
  },
  gcp: {
    icon: 'icon-google',
    displayName: T.translate('features.Market.tabs.gcp'),
  },
  Azure: {
    icon: 'icon-azureblobstore',
    displayName: T.translate('features.Market.tabs.azure'),
  },
  AWS: {
    icon: 'icon-aws',
    displayName: T.translate('features.Market.tabs.aws'),
  },
  Directives: {
    icon: 'icon-directives',
    displayName: T.translate('features.Market.tabs.directives'),
  },
};

const DEFAULT_CATEGORIES = [
  'usecase',
  'pipeline',
  'example',
  'hydrator-plugin',
  'datapack',
  '3rd-party-artifact',
  'EDW Offloading',
  'gcp',
  'Azure',
  'AWS',
  'Directives',
];

export { CATEGORY_MAP, DEFAULT_CATEGORIES };
