/*
 * Copyright © 2019 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
const PREFIX = 'features.Home';

interface ILink {
  label: string;
  url: string;
}

interface IAction {
  img: string;
  title: string;
  description: string;
  links: ILink[];
  experiment?: string;
  featureFlag?: boolean;
}

export const ActionConfig: IAction[] = [
  {
    img: '/cdap_assets/img/cleanse_data.svg',
    title: T.translate(`${PREFIX}.Ingestion.title`).toString(),
    description: T.translate(`${PREFIX}.Ingestion.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Ingestion.linkLabel`).toString(),
        url: `/cdap/ns/:namespace/ingestion`,
      },
    ],
    experiment: 'data-ingestion',
  },
  {
    img: '/cdap_assets/img/cleanse_data.svg',
    title: T.translate(`${PREFIX}.Wrangler.title`).toString(),
    description: T.translate(`${PREFIX}.Wrangler.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Wrangler.linkLabel`).toString(),
        url: `/cdap/ns/:namespace/wrangler`,
      },
    ],
  },
  {
    img: '/cdap_assets/img/data_pipelines.svg',
    title: T.translate(`${PREFIX}.Pipeline.title`).toString(),
    description: T.translate(`${PREFIX}.Pipeline.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Pipeline.studioLabel`).toString(),
        url: '/pipelines/ns/:namespace/studio',
      },
      {
        label: T.translate(`${PREFIX}.Pipeline.listLabel`).toString(),
        url: '/cdap/ns/:namespace/pipelines',
      },
    ],
  },
  {
    img: '/cdap_assets/img/replicator.svg',
    title: T.translate(`${PREFIX}.Replicator.title`).toString(),
    description: T.translate(`${PREFIX}.Replicator.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Replicator.linkLabel`).toString(),
        url: '/cdap/ns/:namespace/replicator',
      },
    ],
    featureFlag: Theme.showCDC,
  },
  {
    img: '/cdap_assets/img/metadata.svg',
    title: T.translate(`${PREFIX}.Metadata.title`).toString(),
    description: T.translate(`${PREFIX}.Metadata.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Metadata.linkLabel`).toString(),
        url: '/metadata/ns/:namespace',
      },
    ],
  },
  {
    img: '/cdap_assets/img/operations.svg',
    title: T.translate(`${PREFIX}.Operations.title`).toString(),
    description: T.translate(`${PREFIX}.Operations.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Operations.linkLabel`).toString(),
        url: '/cdap/ns/:namespace/operations',
      },
    ],
  },
  {
    img: '/cdap_assets/img/administration.svg',
    title: T.translate(`${PREFIX}.Administration.title`).toString(),
    description: T.translate(`${PREFIX}.Administration.description`).toString(),
    links: [
      {
        label: T.translate(`${PREFIX}.Administration.systemLabel`).toString(),
        url: '/cdap/administration',
      },
      {
        label: T.translate(`${PREFIX}.Administration.namespaceLabel`).toString(),
        url: '/cdap/administration/configuration',
      },
    ],
  },
];
