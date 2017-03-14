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
require('./ListViewHeader.scss');
import {DEFAULT_SEARCH_QUERY, DEFAULT_SEARCH_FILTER_OPTIONS} from 'components/EntityListView/SearchStore/SearchConstants';
import SearchStore from 'components/EntityListView/SearchStore';

export default function ListViewHeader() {
  let searchState = SearchStore.getState().search;
  let activeFilters = searchState.activeFilters;
  let activeSort = searchState.activeSort;
  let searchText = searchState.query;
  let filterOptions = DEFAULT_SEARCH_FILTER_OPTIONS;

  const getActiveFilterStrings = () => {
    return activeFilters.map(filter => {
      if (filter === 'app') {
        filter = 'application';
      }
      return T.translate(`commons.entity.${filter}.plural`);
    });
  };
  let text = {
    search: T.translate('features.EntityListView.Info.subtitle.search'),
    filteredBy: T.translate('features.EntityListView.Info.subtitle.filteredBy'),
    sortedBy: T.translate('features.EntityListView.Info.subtitle.sortedBy'),
    displayAll: T.translate('features.EntityListView.Info.subtitle.displayAll'),
    displaySome: T.translate('features.EntityListView.Info.subtitle.displaySome'),
  };

  let i18nResolved_activeFilters = getActiveFilterStrings();
  let allFiltersSelected = (i18nResolved_activeFilters.length === 0 || i18nResolved_activeFilters.length === filterOptions.length);
  let activeFilterString = i18nResolved_activeFilters.join(', ');
  let subtitle;

  if (searchText !== DEFAULT_SEARCH_QUERY) {
    subtitle = `${text.search} "${searchText}"`;
    if (!allFiltersSelected) {
      subtitle += `, ${text.filteredBy} ${activeFilterString}`;
    }
  } else {
    if (allFiltersSelected) {
      subtitle = `${text.displayAll}`;
    } else {
      subtitle = `${text.displaySome} ${activeFilterString}`;
    }
    if (activeSort) {
      subtitle += `, ${text.sortedBy} ${activeSort.displayName}`;
    }
  }

  return (
    <div className="list-view-header subtitle">
      <span>
        {subtitle}
      </span>
    </div>
  );
}
