/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import SearchStore from 'components/EntityListView/SearchStore';
import {search, updateQueryString} from 'components/EntityListView/SearchStore/ActionCreator';
import HomeListView from 'components/EntityListView/ListView';
import MyUserStoreApi from 'api/userstore';
import isNil from 'lodash/isNil';
import EntityListHeader from 'components/EntityListView/EntityListHeader';
import EntityListInfo from 'components/EntityListView/EntityListInfo';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import globalEvents from 'services/global-events';
import ee from 'event-emitter';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import PageErrorMessage from 'components/EntityListView/ErrorMessage/PageErrorMessage';
import HomeErrorMessage from 'components/EntityListView/ErrorMessage';
import Overview from 'components/Overview';
import isEqual from 'lodash/isEqual';
import isEmpty from 'lodash/isEmpty';
import intersection from 'lodash/intersection';
import {objectQuery} from 'services/helpers';
import WelcomeScreen from 'components/EntityListView/WelcomeScreen';
import classnames from 'classnames';
import {
  DEFAULT_SEARCH_FILTERS, DEFAULT_SEARCH_SORT,
  DEFAULT_SEARCH_QUERY, DEFAULT_SEARCH_SORT_OPTIONS,
  DEFAULT_SEARCH_PAGE_SIZE
} from 'components/EntityListView/SearchStore/SearchConstants';

require('./EntityListView.scss');

export default class EntityListView extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entities: [],
      loading: false,
      limit: DEFAULT_SEARCH_PAGE_SIZE,
      total: 0,
      overview: true, // Start showing spinner until we get a response from backend.
      userStoreObj: null,
      showSplash: true
    };
    this.eventEmitter = ee(ee);
    // Maintaining a retryCounter outside the state as it doesn't affect the state/view directly.
    // We just need to retry for 5 times exponentially and then stop with a message.
    this.retryCounter = 0;
    this.refreshSearchByCreationTime = this.refreshSearchByCreationTime.bind(this);
    this.eventEmitter.on(globalEvents.APPUPLOAD, this.refreshSearchByCreationTime);
    this.eventEmitter.on(globalEvents.STREAMCREATE, this.refreshSearchByCreationTime);
    this.eventEmitter.on(globalEvents.PUBLISHPIPELINE, this.refreshSearchByCreationTime);
    this.eventEmitter.on(globalEvents.ARTIFACTUPLOAD, this.refreshSearchByCreationTime);
  }
  componentWillMount() {
    MyUserStoreApi.get().subscribe((res) => {
      let userProperty = typeof res.property === 'object' ? res.property : {};
      let showSplash = userProperty['user-has-visited'] || false;
      this.setState({
        userStoreObj : res,
        showSplash : showSplash
      });
    });
  }
  componentDidMount() {
    this.searchStoreSubscription = SearchStore.subscribe(() => {
      let {
        results:entities,
        loading,
        limit,
        total,
        overviewEntity,
      } = SearchStore.getState().search;
      this.setState({
        entities,
        loading,
        limit,
        total,
        overview: !isNil(overviewEntity)
      });
    });
    SearchStore.dispatch({
      type: SearchStoreActions.SETPAGESIZE,
      payload: {
        element: document.getElementsByClassName('entity-list-view')
      }
    });
    this.parseUrlAndUpdateStore();
  }
  parseUrlAndUpdateStore(nextProps) {
    let props = nextProps || this.props;
    let queryObject = this.getQueryObject(props.location.query);
    let pageSize = SearchStore.getState().search.limit;
    SearchStore.dispatch({
      type: SearchStoreActions.SETSORTFILTERSEARCHCURRENTPAGE,
      payload: {
        activeSort: queryObject.sort,
        activeFilters: queryObject.filters,
        query: queryObject.query,
        currentPage: queryObject.page,
        offset: (queryObject.page - 1) * pageSize,
        overviewEntity: queryObject.overview
      }
    });
    search();
  }
  componentWillReceiveProps(nextProps) {
    let searchState = SearchStore.getState().search;
    if (nextProps.currentPage !== searchState.currentPage) {
      // To enable explore fastaction on each card in entity list page.
      ExploreTablesStore.dispatch(
       fetchTables(nextProps.params.namespace)
     );
    }

    let queryObject = this.getQueryObject(nextProps.location.query);
    if (
      (nextProps.params.namespace !== this.props.params.namespace) ||
      (
        !isEqual(queryObject.filters, searchState.activeFilters) ||
        queryObject.sort.fullSort !== searchState.activeSort.fullSort ||
        queryObject.query !== searchState.query ||
        queryObject.page !== searchState.currentPage ||
        objectQuery(queryObject, 'overview', 'id') !== objectQuery(searchState, 'overviewEntity', 'id') ||
        objectQuery(queryObject, 'overview', 'type') !== objectQuery(searchState, 'overviewEntity', 'type')
      )
    ) {
      if ((nextProps.params.namespace !== this.props.params.namespace)) {
        NamespaceStore.dispatch({
          type: NamespaceActions.selectNamespace,
          payload: {
            selectedNamespace: nextProps.params.namespace
          }
        });
      }
      this.parseUrlAndUpdateStore(nextProps);
    }
  }
  componentWillUnmount() {
    SearchStore.dispatch({
      type: SearchStoreActions.RESETSTORE
    });
    if (this.searchStoreSubscription) {
      this.searchStoreSubscription();
    }
    this.eventEmitter.off(globalEvents.APPUPLOAD, this.refreshSearchByCreationTime);
    this.eventEmitter.off(globalEvents.STREAMCREATE, this.refreshSearchByCreationTime);
    this.eventEmitter.off(globalEvents.PUBLISHPIPELINE, this.refreshSearchByCreationTime);
    this.eventEmitter.off(globalEvents.ARTIFACTUPLOAD, this.refreshSearchByCreationTime);
  }
  refreshSearchByCreationTime() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    ExploreTablesStore.dispatch(
     fetchTables(namespace)
   );
   SearchStore.dispatch({
     type: SearchStoreActions.SETACTIVESORT,
     payload: {
       activeSort: SearchStore.getState().search.sort[4]
     }
   });
   search();
   updateQueryString();
  }
  getQueryObject(query) {
    if (isNil(query)) {
      query = {};
    }
    let {
      q = '*',
      sort=DEFAULT_SEARCH_SORT.sort,
      order=DEFAULT_SEARCH_SORT.order,
      filter=DEFAULT_SEARCH_FILTERS,
      page=1,
      overviewid = null,
      overviewtype = null
    } = query;
    const getSort = (sortOption, order, q) => {
      let isValidSortOption = DEFAULT_SEARCH_SORT_OPTIONS.find(sortOpt => sortOpt.sort === sortOption && sortOpt.order === order);
      if (!isValidSortOption) {
        return DEFAULT_SEARCH_SORT;
      }
      if (q !== DEFAULT_SEARCH_QUERY) {
        return DEFAULT_SEARCH_SORT_OPTIONS[0];
      }
      return isValidSortOption;
    };
    const getFilters = (filters) => {
      if (!Array.isArray(filters)) {
        filters = [filters];
      }
      let validFilters = intersection(filters, DEFAULT_SEARCH_FILTERS);
      if (!validFilters.length) {
        return DEFAULT_SEARCH_FILTERS;
      }
      return validFilters;
    };
    const getPageNum = (page) => {
      if (isNaN(page)) {
        return 1;
      }
      return parseInt(page, 10);
    };
    const getSearchQuery = (q) => {
      if (isNil(q) || isEmpty(q)) {
        return DEFAULT_SEARCH_QUERY;
      }
      return q;
    };
    const getOverviewEntity = (overviewid, overviewtype) => {
      if (!isNil(overviewid) && !isNil(overviewtype)) {
        return {
          id: overviewid,
          type: overviewtype
        };
      }
      return null;
    };
    let queryObject = {
      sort: getSort(sort, order, q),
      filters: getFilters(filter),
      page: getPageNum(page),
      query: getSearchQuery(q),
      overview: getOverviewEntity(overviewid, overviewtype)
    };
    return queryObject;
  }
  retrySearch() {
    this.retryCounter += 1;
    search();
  }
  onOverviewCloseAndRefresh() {
    this.setState({
      overview: false
    });
    SearchStore.dispatch({
      type: SearchStoreActions.RESETOVERVIEWENTITY
    });
    search();
  }
  dismissSplash() {
    this.setState({
      showSplash: true
    });
    this.parseUrlAndUpdateStore();
  }
  render() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let searchState = SearchStore.getState();
    let currentPage = searchState.search.currentPage;
    let query = searchState.search.query;
    let searchText = searchState.search.query;
    let numCursors = searchState.search.numCursors;
    let offset = searchState.search.offset;
    let {statusCode:errorStatusCode, message:errorMessage } = searchState.search.error;
    let errorContent;

    if (!this.state.showSplash) {
      return (
        <WelcomeScreen
          onClose={this.dismissSplash.bind(this)}
        />
      );
    }
    if (!isNil(errorStatusCode)) {
      if (errorStatusCode === 'PAGE_NOT_FOUND') {
        errorContent = (
          <PageErrorMessage
            pageNum={currentPage}
            query={query}
          />
        );
      } else {
        errorContent = (
          <HomeErrorMessage
            errorMessage={errorMessage}
            errorStatusCode={errorStatusCode}
            onRetry={this.retrySearch.bind(this)}
            retryCounter={this.retryCounter}
          />
        );
      }
    }

    return (
      <div>
        <EntityListHeader />
        <div className="entity-list-view">
          {
            !isNil(errorContent) ?
              null
            :
              <EntityListInfo
                className="entity-list-info"
                namespace={namespace}
                numberOfEntities={this.state.total}
                numberOfPages={this.state.total / this.state.limit}
                currentPage={currentPage}
                allEntitiesFetched = {this.state.total < (this.state.limit * (numCursors + 1) + offset)}
              />
          }
          <div className={classnames("entities-container", {'error-holder': errorContent})}>
            {
              !isNil(errorContent) ?
                errorContent
              :
                <HomeListView
                  id="home-list-view-container"
                  loading={this.state.loading}
                  className={classnames("home-list-view-container", {"show-overview-main-container": this.state.overview})}
                  list={this.state.entities}
                  pageSize={this.state.limit}
                  showJustAddedSection={searchText === DEFAULT_SEARCH_QUERY}
                  onFastActionSuccess={search}
                />
            }
            <Overview
              onCloseAndRefresh={this.onOverviewCloseAndRefresh.bind(this)}
            />
          </div>
        </div>
      </div>
    );
  }
}

EntityListView.propTypes = {
  params: PropTypes.shape({
    namespace : PropTypes.string
  }),
  location: PropTypes.object,
  history: PropTypes.object,
  pathname: PropTypes.string
};
