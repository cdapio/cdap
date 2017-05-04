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

import {combineReducers, createStore} from 'redux';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import {
  DEFAULT_SEARCH_QUERY,
  DEFAULT_SEARCH_FILTER_OPTIONS,
  DEFAULT_SEARCH_FILTERS,
  DEFAULT_SEARCH_SORT,
  DEFAULT_SEARCH_SORT_OPTIONS,
  DEFAULT_SEARCH_PAGE_SIZE
} from 'components/EntityListView/SearchStore/SearchConstants';
import isNil from 'lodash/isNil';

const defaultAction = {
  type: {},
  payload: {}
};

const defaultErrorState = {
  statusCode: null,
  message: ''
};

const defaultSearchState = {
  filters: DEFAULT_SEARCH_FILTER_OPTIONS,
  activeFilters: DEFAULT_SEARCH_FILTERS,
  sort: DEFAULT_SEARCH_SORT_OPTIONS,
  activeSort: DEFAULT_SEARCH_SORT,
  query: DEFAULT_SEARCH_QUERY,

  offset: 0,
  limit: DEFAULT_SEARCH_PAGE_SIZE,
  numColumns: null,
  numCursors: 10,
  total: 0,
  currentPage: 1,

  loading: false,
  results: [],

  error: defaultErrorState,

  overviewEntity: null
};

const defaultInitialState = {
  search: defaultSearchState
};

const getPageSize = (element) => {
    // different screen sizes
    // consistent with media queries in style sheet
    const sevenColumnWidth = 1701;
    const sixColumnWidth = 1601;
    const fiveColumnWidth = 1201;
    const fourColumnWidth = 993;
    const threeColumnWidth = 768;

    // 140 = cardHeight (128) + (5 x 2 top bottom margins) + (1 x 2 border widths)
    const cardHeightWithMarginAndBorder = 140;

    let entityListViewEle = element;

    if (!entityListViewEle.length) {
      return {};
    }

    // Subtract 65px to account for entity-list-info's height (45px) and paddings (20px)
    // minus 10px of padding from top and bottom (10px each)
    let containerHeight = entityListViewEle[0].offsetHeight - 65 - 10;
    let containerWidth = entityListViewEle[0].offsetWidth;
    let numColumns = 1;

    // different screen sizes
    // consistent with media queries in style sheet
    if (containerWidth >= sevenColumnWidth) {
      numColumns = 7;
    } else if (containerWidth >= sixColumnWidth && containerWidth < sevenColumnWidth) {
      numColumns = 6;
    } else if (containerWidth >= fiveColumnWidth && containerWidth < sixColumnWidth) {
      numColumns = 5;
    } else if (containerWidth >= fourColumnWidth && containerWidth < fiveColumnWidth) {
      numColumns = 4;
    } else if (containerWidth >= threeColumnWidth && containerWidth < fourColumnWidth) {
      numColumns = 3;
    }

    let numRows = Math.floor(containerHeight / cardHeightWithMarginAndBorder);

    // We must have one column and one row at the very least
    if (numRows === 0) {
      numRows = 1;
    }
    return {
      numColumns,
      limit: numColumns * numRows
    };
};

const search = (state = defaultSearchState, action = defaultAction) => {
  switch (action.type) {
    case SearchStoreActions.SETRESULTS: {
      let {results, total, limit} = action.payload.response;
      return Object.assign({}, state, {
        results: results,
        total,
        limit,
        loading: false,
        error: {}
      });
    }
    case SearchStoreActions.SETACTIVEFILTERS:
      return Object.assign({}, state, {
        activeFilters: action.payload.activeFilters,
        overviewEntity: null
      });
    case SearchStoreActions.SETACTIVESORT:
      if (isNil(action.payload.activeSort)) {
        return state;
      }
      return Object.assign({}, state, {
        activeSort: action.payload.activeSort,
        overviewEntity: null,
        query: action.payload.query || state.query
      });
    case SearchStoreActions.SETQUERY:
      return Object.assign({}, state, {
        query: action.payload.query === '' ? '*' : action.payload.query,
        currentPage: 1,
        offset: 0,
        activeSort: action.payload.query !== '*' ? DEFAULT_SEARCH_SORT_OPTIONS[0] : state.activeSort,
        overviewEntity: ['', '*'].indexOf(action.payload.query) !== -1 ? state.overviewEntity : null
      });
    case SearchStoreActions.LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading || false
      });
    case SearchStoreActions.SETPAGESIZE: {
      let {limit = DEFAULT_SEARCH_PAGE_SIZE, numColumns = 10} = getPageSize(action.payload.element);
      return Object.assign({}, state, {
        limit,
        numColumns
      });
    }
    case SearchStoreActions.SETCURRENTPAGE:
      return Object.assign({}, state, {
        currentPage: action.payload.currentPage,
        offset: action.payload.offset,
        overviewEntity: null
      });
    case SearchStoreActions.RESETERROR:
      return Object.assign({}, state, {
        error: defaultErrorState
      });
    case SearchStoreActions.SETERROR:
      return Object.assign({}, state, {
        error: {
          statusCode: action.payload.errorStatusCode,
          message: action.payload.errorMessage
        }
      });
    case SearchStoreActions.RESETOVERVIEWENTITY:
      return Object.assign({}, state, {
        overviewEntity: null
      });
    case SearchStoreActions.SETOVERVIEWENTITY:
      return Object.assign({}, state, {
        overviewEntity: action.payload.overviewEntity
      });
    case SearchStoreActions.SETSORTFILTERSEARCHCURRENTPAGE:
      return Object.assign({}, state, {
        query: action.payload.query === '' ? '*' : action.payload.query,
        activeSort: action.payload.query !== '*' ? DEFAULT_SEARCH_SORT_OPTIONS[0] : action.payload.activeSort,
        activeFilters: action.payload.activeFilters,
        currentPage: action.payload.currentPage,
        offset: action.payload.offset,
        overviewEntity: action.payload.overviewEntity,
        error: {}
      });
    case SearchStoreActions.RESETSTORE:
      return defaultSearchState;
    default:
      return state;
  }
};

const searchStoreWrapper = () => {
  return createStore(
    combineReducers({
      search
    }),
    defaultInitialState
  );
};

const SearchStore = searchStoreWrapper();
export default SearchStore;
