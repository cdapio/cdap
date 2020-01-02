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
import T from 'i18n-react';

const DEFAULT_SEARCH_FILTER_OPTIONS = [
  {
    displayName: T.translate('commons.entity.application.plural'),
    id: 'application',
  },
  {
    displayName: T.translate('commons.entity.artifact.plural'),
    id: 'artifact',
  },
  {
    displayName: T.translate('commons.entity.dataset.plural'),
    id: 'dataset',
  },
];

const DEFAULT_SEARCH_FILTERS = DEFAULT_SEARCH_FILTER_OPTIONS.filter((f) => {
  return f.id !== 'artifact';
}).map((filter) => filter.id);

const DEFAULT_SEARCH_SORT_OPTIONS = [
  {
    displayName: T.translate('features.EntityListView.Header.sortOptions.none'),
    sort: 'none',
    fullSort: 'none',
  },
  {
    displayName: T.translate(
      'features.EntityListView.Header.sortOptions.entityNameAsc.displayName'
    ),
    sort: 'name',
    order: 'asc',
    fullSort: 'entity-name asc',
  },
  {
    displayName: T.translate(
      'features.EntityListView.Header.sortOptions.entityNameDesc.displayName'
    ),
    sort: 'name',
    order: 'desc',
    fullSort: 'entity-name desc',
  },
  {
    displayName: T.translate(
      'features.EntityListView.Header.sortOptions.creationTimeAsc.displayName'
    ),
    sort: 'creation-time',
    order: 'asc',
    fullSort: 'creation-time asc',
  },
  {
    displayName: T.translate(
      'features.EntityListView.Header.sortOptions.creationTimeDesc.displayName'
    ),
    sort: 'creation-time',
    order: 'desc',
    fullSort: 'creation-time desc',
  },
];

const DEFAULT_SEARCH_SORT = DEFAULT_SEARCH_SORT_OPTIONS[4];

const DEFAULT_SEARCH_QUERY = '*';

const DEFAULT_SEARCH_PAGE_SIZE = 30;

const JUSTADDED_THRESHOLD_TIME = 300000; // 5 minutes in millisecond
export {
  DEFAULT_SEARCH_FILTER_OPTIONS,
  DEFAULT_SEARCH_PAGE_SIZE,
  DEFAULT_SEARCH_FILTERS,
  DEFAULT_SEARCH_SORT,
  DEFAULT_SEARCH_SORT_OPTIONS,
  DEFAULT_SEARCH_QUERY,
  JUSTADDED_THRESHOLD_TIME,
};
