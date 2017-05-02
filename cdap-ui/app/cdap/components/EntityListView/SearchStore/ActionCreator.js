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

import {MySearchApi} from 'api/search';
import NamespaceStore from 'services/NamespaceStore';
import {parseMetadata} from 'services/metadata-parser';
import shortid from 'shortid';
import SearchStore from 'components/EntityListView/SearchStore';
import SearchStoreAction from 'components/EntityListView/SearchStore/SearchStoreActions';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import {DEFAULT_SEARCH_QUERY} from 'components/EntityListView/SearchStore/SearchConstants';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import isNil from 'lodash/isNil';

const search = () => {
  let namespace = NamespaceStore.getState().selectedNamespace;
  let {
    offset,
    numCursors,
    limit,
    activeFilters,
    activeSort,
    query
  } = SearchStore.getState().search;

  let params = {
    namespace: namespace,
    target: activeFilters,
    limit,
    offset,
    numCursors,
    sort: activeSort.fullSort,
    query
  };
  if (query !== DEFAULT_SEARCH_QUERY) {
    delete params.sort;
    delete params.numCursors;
    params.query = params.query + '*';
  }

  ExploreTablesStore.dispatch(
    fetchTables(namespace)
  );

  SearchStore.dispatch({
    type: SearchStoreAction.LOADING,
    payload: {
      loading: true
    }
  });

  MySearchApi.search(params)
    .map((res) => {
      return Object.assign({}, {
        total: res.total,
        limit: res.limit,
        results: res.results
          .map(parseMetadata)
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          })
      });
    })
    .subscribe(
      (response) => {
        let currentPage = SearchStore.getState().search.currentPage;
        if (response.total > 0 && Math.ceil(response.total/limit) < currentPage) {
          SearchStore.dispatch({
            type: SearchStoreActions.SETERROR,
            payload: {
              errorStatusCode: 'PAGE_NOT_FOUND',
              errorMessage: null
            }
          });
          return;
        }
        SearchStore.dispatch({
          type: SearchStoreAction.SETRESULTS,
          payload: {response}
        });
      },
      (error) => {
        SearchStore.dispatch({
          type: SearchStoreActions.SETERROR,
          payload: {
            errorStatusCode: error.statusCode,
            errorMessage: typeof error === 'object' ? error.response : error,
          }
        });
      }
    );
};

const updateQueryString = () => {
  let queryString = '';
  let sort = '';
  let filter = '';
  let query = '';
  let page = '';
  let queryParams = [];

  let searchState = SearchStore.getState().search;
  let {activeSort, activeFilters, query:searchQuery, currentPage, overviewEntity} = searchState;

  // Generate sort params
  if (activeSort.sort !== 'none') {
    sort = 'sort=' + activeSort.sort + '&order=' + activeSort.order;
  }

  // Generate filter params
  if (activeFilters.length === 1) {
    filter = 'filter=' + activeFilters[0];
  } else if (activeFilters.length > 1) {
    filter = 'filter=' + activeFilters.join('&filter=');
  }

  // Generate search param
  if (searchQuery.length > 0) {
    query = 'q=' + searchQuery;
  }

  // Generate page param
  page = 'page=' + currentPage;

  // Combine query parameters into query string
  queryParams = [query, sort, filter, page].filter((element) => {
    return element.length > 0;
  });
  queryString = queryParams.join('&');

  if (queryString.length > 0) {
    queryString = '?' + queryString;
  }

  if (!isNil(overviewEntity)) {
    queryString += `&overviewid=${overviewEntity.id}&overviewtype=${overviewEntity.type}`;
  }

  let obj = {
    title: 'CDAP',
    url: location.pathname + queryString
  };

  // Modify URL to match application state
  history.pushState(obj, obj.title, obj.url);
};
export {
  search,
  updateQueryString
};
