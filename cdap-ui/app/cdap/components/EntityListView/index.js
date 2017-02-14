/*
 * Copyright © 2016 Cask Data, Inc.
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
import {MySearchApi} from 'api/search';
import {parseMetadata} from 'services/metadata-parser';
import EntityListHeader from './EntityListHeader';
import Pagination from 'components/Pagination';
import T from 'i18n-react';
const shortid = require('shortid');
const classNames = require('classnames');
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import MyUserStoreApi from 'api/userstore';
import PlusButtonStore from 'services/PlusButtonStore';
import globalEvents from 'services/global-events';
import isNil from 'lodash/isNil';
import Overview from 'components/Overview';
import EntityListInfo from './EntityListInfo';
import WelcomeScreen from 'components/EntityListView/WelcomeScreen';
import HomeListView from 'components/EntityListView/ListView';
import NamespaceStore from 'services/NamespaceStore';
import Page404 from 'components/404';
import PageErrorMessage from 'components/EntityListView/ErrorMessage/PageErrorMessage';
import HomeErrorMessage from 'components/EntityListView/ErrorMessage';
import isEqual from 'lodash/isEqual';
require('./EntityListView.scss');
import ee from 'event-emitter';

const defaultFilter = ['app', 'artifact', 'dataset', 'stream'];

class EntityListView extends Component {
  constructor(props) {
    super(props);
    this.filterOptions = [
      {
        displayName: T.translate('commons.entity.application.plural'),
        id: 'app'
      },
      {
        displayName: T.translate('commons.entity.artifact.plural'),
        id: 'artifact'
      },
      {
        displayName: T.translate('commons.entity.dataset.plural'),
        id: 'dataset'
      },
      {
        displayName: T.translate('commons.entity.stream.plural'),
        id: 'stream'
      }
    ];

    // Accepted filter ids to be compared against incoming query parameters
    this.acceptedFilterIds = this.filterOptions.map( (item) => {
      return item.id;
    });

    this.sortOptions = [
      {
        displayName: T.translate('features.EntityListView.Header.sortOptions.none'),
        sort: 'none',
        fullSort: 'none'
      },
      {
        displayName: T.translate('features.EntityListView.Header.sortOptions.entityNameAsc.displayName'),
        sort: 'name',
        order: 'asc',
        fullSort: 'entity-name asc'
      },
      {
        displayName: T.translate('features.EntityListView.Header.sortOptions.entityNameDesc.displayName'),
        sort: 'name',
        order: 'desc',
        fullSort: 'entity-name desc'
      },
      {
        displayName: T.translate('features.EntityListView.Header.sortOptions.creationTimeAsc.displayName'),
        sort: 'creation-time',
        order: 'asc',
        fullSort: 'creation-time asc'
      },
      {
        displayName: T.translate('features.EntityListView.Header.sortOptions.creationTimeDesc.displayName'),
        sort: 'creation-time',
        order: 'desc',
        fullSort: 'creation-time desc'
      }
    ];

    this.state = {
      filter: defaultFilter,
      sortObj: this.sortOptions[4],
      query: '',
      entities: [],
      selectedEntity: null,
      numPages: 1,
      loading: true,
      currentPage: 1,
      animationDirection: 'next',
      showSplash: true,
      userStoreObj : '',
      notFound: false,
      numColumns: null
    };

    this.retryCounter = 0; // being used for search API retry

    // By default, expect a single page -- update when search is performed and we can parse it
    this.pageSize = 1;
    this.dismissSplash = this.dismissSplash.bind(this);
    this.calculatePageSize = this.calculatePageSize.bind(this);
    this.updateQueryString = this.updateQueryString.bind(this);
    this.getQueryObject = this.getQueryObject.bind(this);
    this.handlePageChange = this.handlePageChange.bind(this);
    this.setAnimationDirection = this.setAnimationDirection.bind(this);
    this.eventEmitter = ee(ee);
    this.refreshSearchByCreationTime = this.refreshSearchByCreationTime.bind(this);
    this.eventEmitter.on(globalEvents.APPUPLOAD, this.refreshSearchByCreationTime);
    this.eventEmitter.on(globalEvents.STREAMCREATE, this.refreshSearchByCreationTime);
    this.eventEmitter.on(globalEvents.PUBLISHPIPELINE, this.refreshSearchByCreationTime);
  }

  refreshSearchByCreationTime() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    ExploreTablesStore.dispatch(
     fetchTables(namespace)
   );
    this.setState({
      sortObj: this.sortOptions[4],
      selectedEntity: null
    }, this.search.bind(this));
  }

  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.APPUPLOAD, this.refreshSearchByCreationTime);
    this.eventEmitter.off(globalEvents.STREAMCREATE, this.refreshSearchByCreationTime);
    this.eventEmitter.off(globalEvents.PUBLISHPIPELINE, this.refreshSearchByCreationTime);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.currentPage !== this.state.currentPage) {
      // To enable explore fastaction on each card in entity list page.
      ExploreTablesStore.dispatch(
       fetchTables(nextProps.params.namespace)
     );
    }

    let queryObject = this.getQueryObject(nextProps.location.query);
    if (
      (nextProps.params.namespace !== this.props.params.namespace) ||
      (
        !isEqual(queryObject.filter, this.state.filter) ||
        queryObject.sort.fullSort !== this.state.sortObj.fullSort ||
        queryObject.query !== this.state.query ||
        queryObject.page !== this.state.currentPage
      )
    ) {
      this.updateData(queryObject.query, queryObject.filter, queryObject.sort, nextProps.params.namespace, queryObject.page);
      this.setState({
        filter: queryObject.filter,
        sortObj: queryObject.sort,
        query: queryObject.query,
        currentPage: queryObject.page,
        loading: true,
        entityErr: false,
        errStatusCode: null
      });
    }
  }

  // Update Store and State to correspond to query parameters before component renders
  componentWillMount() {
    MyUserStoreApi.get().subscribe((res) => {
      let userProperty = typeof res.property === 'object' ? res.property : {};
      let showSplash = userProperty['user-has-visited'] || false;
      this.setState({
        userStoreObj : res,
        showSplash : showSplash
      });
    });

    // To enable explore fastaction on each card in Entity list view
     ExploreTablesStore.dispatch(
      fetchTables(this.props.params.namespace)
    );

    // Process and return valid query parameters
    let queryObject = this.getQueryObject(this.props.location.query);
    this.setState({
      filter: queryObject.filter,
      sortObj: queryObject.sort,
      query: queryObject.query,
      currentPage: queryObject.page
    });
  }

  // Performs calculations to determine number of entities to render per page
  calculatePageSize() {
    // different screen sizes
    // consistent with media queries in style sheet
    const sevenColumnWidth = 1701;
    const sixColumnWidth = 1601;
    const fiveColumnWidth = 1201;
    const fourColumnWidth = 993;
    const threeColumnWidth = 768;

    // 140 = cardHeight (128) + (5 x 2 top bottom margins) + (1 x 2 border widths)
    const cardHeightWithMarginAndBorder = 140;

    let entityListViewEle = document.getElementsByClassName('entity-list-view');

    if (!entityListViewEle.length) {
      return;
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

    this.setState({numColumns});

    let numRows = Math.floor(containerHeight / cardHeightWithMarginAndBorder);

    // We must have one column and one row at the very least
    if (numRows === 0) {
      numRows = 1;
    }

    this.pageSize = numColumns * numRows;
  }

  // Retrieve entities for rendering
  componentDidMount() {
    let namespaces = NamespaceStore.getState().namespaces.map(ns => ns.name);
    if (namespaces.length) {
      let selectedNamespace = NamespaceStore.getState().selectedNamespace;
      if (namespaces.indexOf(selectedNamespace) === -1) {
        this.setState({
          notFound: true,
          loading: false
        });
      } else {
        this.calculatePageSize();
        this.updateData();
      }
    } else {
      const namespaceSubcriber = () => {
        let selectedNamespace = NamespaceStore.getState().selectedNamespace;
        let namespaces = NamespaceStore.getState().namespaces.map(ns => ns.name);
        if (namespaces.length && namespaces.indexOf(selectedNamespace) === -1) {
          this.setState({
            notFound: true
          });
        } else {
          this.setState({
            notFound: false,
            currentPage: this.state.currentPage
          }, () => {
            this.calculatePageSize();
            this.search();
            this.namespaceStoreSubscription();
          });
        }
      };
      this.namespaceStoreSubscription = NamespaceStore.subscribe(namespaceSubcriber.bind(this));
    }
  }

  // Construct and return query object from query parameters
  getQueryObject(query) {
    let sortBy = '';
    let orderBy = '';
    let searchTerm = '';
    let sortOption = '';
    let filters = '';
    let page = this.state.currentPage;
    let verifiedFilters = null;
    let invalidFilter = false;

    // Get filters, order, sort, search from query
    if (query) {
      if (typeof query.q === 'undefined') {
        sortBy = typeof query.sort === 'string' ? query.sort : '';
        orderBy = typeof query.order === 'string' ? query.order : '';
      }
      searchTerm = typeof query.q === 'string' ? query.q : '';
      page = isNaN(query.page) ? this.state.currentPage : Number(query.page);

      if (page <= 0) {
        page = 1;
      }

      if (typeof query.filter === 'string') {
        filters = [query.filter];
      } else if (Array.isArray(query.filter)) {
        filters = query.filter;
      }
    }

    // Ensure sort parameters are valid
    sortOption = this.sortOptions.filter((option) => {
      return ( sortBy === option.sort && orderBy === option.order);
    });

    // Ensure filter parameters are valid
    if (filters.length > 0) {
      verifiedFilters = filters.filter( (filterOption) => {
        if (this.acceptedFilterIds.indexOf(filterOption) !== -1) {
          return true;
        } else {
          invalidFilter = true;
          return false;
        }
      });
    }

    // Ensure all defaults are applied if an invalid parameter is passed
    if (invalidFilter) {
      defaultFilter.forEach(( option ) => {
        if (verifiedFilters.indexOf(option) === -1) {
          verifiedFilters.push(option);
        }
      });
    }
    let sort;
    if (!searchTerm) {
      sort = sortOption.length === 0 ? this.state.sortObj : sortOption[0];
    } else {
      sort = this.sortOptions[0];
    }
    // Return valid query parameters or return current state values if query params are invalid
    return ({
      'query' : searchTerm ? searchTerm : this.state.query,
      'sort': sort,
      'filter' : verifiedFilters ? verifiedFilters : this.state.filter,
      'page' : page
    });
  }

  updateData(
    query = this.state.query,
    filter = this.state.filter,
    sortObj = this.state.sortObj,
    namespace = this.props.params.namespace,
    currentPage = this.state.currentPage
  ) {
    let offset = (currentPage - 1) * this.pageSize;

    // TODO/FIXME: hack to not display programs when filter is empty (which means all
    // entities should be displayed). Maybe this should be a backend change?
    if (filter.length === 0) {
      filter = ['app', 'artifact', 'dataset', 'stream'];
    }

    this.setState({
      loading : true,
      selectedEntity: null
    });

    let params = {
      namespace: namespace,
      target: filter,
      limit: this.pageSize,
      offset: offset
    };

    if (typeof query === 'string' && query.length) {
      params.query = `${query}*`;
    } else {
      params.sort = sortObj.fullSort === 'none' ? this.sortOptions[3].fullSort : sortObj.fullSort;
      params.query = '*';
      params.numCursors = 10;
    }

    let total, limit;
    MySearchApi.search(params)
      .map((res) => {
        total = res.total;
        limit = res.limit;
        return res.results
          .map(parseMetadata)
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          });
      })
      .subscribe((res) => {
        if (total > 0 && Math.ceil(total/limit) < this.state.currentPage) {
          this.setState({
            entityErr: true,
            errStatusCode: 'PAGE_NOT_FOUND',
            loading: false
          });
        } else {
          this.setState({
            entities: res,
            total: total,
            loading: false,
            entityErr: false,
            errStatusCode: null,
            numPages: Math.ceil(total / this.pageSize)
          });

          this.retryCounter = 0;
        }
      }, (err) => {
        this.retryCounter++;

        // On Error: render page as if there are no results found
        this.setState({
          loading: false,
          entityErr: typeof err === 'object' ? err.response : err,
          errStatusCode: err.statusCode
        });
      });
  }
  search(
    query = this.state.query,
    filter = this.state.filter,
    sortObj = this.state.sortObj,
    namespace = this.props.params.namespace,
    currentPage = this.state.currentPage
  ) {
    this.updateData(query, filter, sortObj, namespace, currentPage);
    this.updateQueryString();
  }

  handleFilterClick(option) {
    let filters = [...this.state.filter];
    if (this.state.filter.indexOf(option.id) !== -1) {
      let index = filters.indexOf(option.id);
      filters.splice(index, 1);
    } else {
      filters.push(option.id);
    }

    this.setState({
      filter : filters,
      selectedEntity: null,
      currentPage: 1
    }, () => {
      this.search(this.state.query, filters, this.state.sortObj);
    });
  }

  handlePageChange(pageNumber) {
    if (pageNumber < 1 || pageNumber > this.state.numPages) {
      return;
    }

    let direction = pageNumber >= this.state.currentPage ? 'next' : 'prev';

    this.setState({
      currentPage : pageNumber,
      animationDirection : direction,
      selectedEntity: null
    }, () => this.search());
  }

  handleSortClick(option) {
    this.setState({
      sortObj : option,
      query: '',
      selectedEntity: null,
      currentPage: 1
    }, () => {
      this.search(this.state.query, this.state.filter, option);
    });
  }

  handleSearch(query) {
    let sortObj = this.sortOptions[0];
    if (query.length === 0) {
      sortObj = this.sortOptions[4]; // search text is empty, revert to default ('Newest')
    }
    this.setState({
      query,
      sortObj,
      selectedEntity: null,
      currentPage: this.state.currentPage
    }, () => {
      this.search(query, this.state.filter, this.state.sortObj);
    });
  }

  handleEntityClick(entity) {
    this.setState({selectedEntity: entity});
  }

  // Set query string using current application state
  updateQueryString() {
    let queryString = '';
    let sort = '';
    let filter = '';
    let query = '';
    let page = '';
    let queryParams = [];

    // Generate sort params
    if (this.state.sortObj.sort !== 'none') {
      sort = 'sort=' + this.state.sortObj.sort + '&order=' + this.state.sortObj.order;
    }

    // Generate filter params
    if (this.state.filter.length === 1) {
      filter = 'filter=' + this.state.filter[0];
    } else if (this.state.filter.length > 1) {
      filter = 'filter=' + this.state.filter.join('&filter=');
    }

    // Generate search param
    if (this.state.query.length > 0) {
      query = 'q=' + this.state.query;
    }

    // Generate page param
    page = 'page=' + this.state.currentPage;

    // Combine query parameters into query string
    queryParams = [query, sort, filter, page].filter((element) => {
      return element.length > 0;
    });
    queryString = queryParams.join('&');

    if (queryString.length > 0) {
      queryString = '?' + queryString;
    }

    let obj = {
      title: 'CDAP',
      url: location.pathname + queryString
    };

    // Modify URL to match application state
    history.pushState(obj, obj.title, obj.url);
  }

  setAnimationDirection(direction) {
    this.setState({
      animationDirection : direction,
      selectedEntity: null
    });
  }

  dismissSplash() {
    MyUserStoreApi
      .get()
      .subscribe(res => {
        res.property['user-has-visited'] = true;
        MyUserStoreApi.set({}, res.property);
      });
    this.setState({
      showSplash: true
    });
  }

  openMarketModal() {
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: true
      }
    });
  }

  onOverviewCloseAndRefresh() {
    this.setState({selectedEntity: null}, this.search.bind(this));
  }

  onFastActionSuccess() {
    if (this.state.selectedEntity) {
      this.onOverviewCloseAndRefresh();
      return;
    }
    this.search();
  }

  render() {
    if (this.state.notFound) {
      return (
        <div className="entity-list-view">
          <Page404
            entityType="Namespace"
            entityName={this.props.params.namespace}
          >
            <div className="namespace-not-found text-xs-center">
              <h4>
                <strong>
                  {T.translate('features.EntityListView.NamespaceNotFound.createMessage')}
                  <span
                    className="open-namespace-wizard-link"
                    onClick={() => {
                      this.eventEmitter.emit(globalEvents.CREATENAMESPACE);
                    }}
                  >
                    {T.translate('features.EntityListView.NamespaceNotFound.createLinkLabel')}
                  </span>
                </strong>
              </h4>
              <h4>
                <strong>
                  {T.translate('features.EntityListView.NamespaceNotFound.switchMessage')}
                </strong>
              </h4>
            </div>
          </Page404>
        </div>
      );
    }
    if (!this.state.showSplash) {
      return (
        <WelcomeScreen
          onClose={this.dismissSplash.bind(this)}
          onMarketOpen={this.openMarketModal.bind(this)}
        />
      );
    }

    let errorContent = null;
    if (this.state.entityErr) {
      if (this.state.errStatusCode === 'PAGE_NOT_FOUND') {
        errorContent = (
          <PageErrorMessage
            pageNum={this.state.currentPage}
            query={this.state.query}
          />
        );
      } else {
        errorContent = (
          <HomeErrorMessage
            errorMessage={this.state.entityErr}
            errorStatusCode={this.state.errStatusCode}
            onRetry={this.search.bind(this)}
            retryCounter={this.retryCounter}
          />
        );
      }
    }

    return (
      <div>
        <EntityListHeader
          filterOptions={this.filterOptions}
          onFilterClick={this.handleFilterClick.bind(this)}
          activeFilter={this.state.filter}
          sortOptions={this.sortOptions}
          activeSort={this.state.sortObj}
          onSortClick={this.handleSortClick.bind(this)}
          onSearch={this.handleSearch.bind(this)}
          searchText={this.state.query}
          onPageChange={this.handlePageChange}
        />
        <Pagination
          className="entity-list-view"
          setCurrentPage={this.handlePageChange}
          totalPages={this.state.numPages}
          currentPage={this.state.currentPage}
          setDirection={this.setAnimationDirection}
        >
          <EntityListInfo
            className="entity-list-info"
            namespace={this.props.params.namespace}
            numberOfEntities={this.state.total}
            numberOfPages={this.state.numPages}
            currentPage={this.state.currentPage}
            onPageChange={this.handlePageChange}
          />
          <div className={classNames("entities-container")}>
            {
              this.state.entityErr ?
                errorContent
              :
                <HomeListView
                  className={classNames("home-list-view-container", {"show-overview-main-container": !isNil(this.state.selectedEntity)})}
                  list={this.state.entities}
                  loading={this.state.loading}
                  onEntityClick={this.handleEntityClick.bind(this)}
                  onFastActionSuccess={this.onFastActionSuccess.bind(this)}
                  errorMessage={this.state.entityErr}
                  errorStatusCode={this.state.errStatusCode}
                  animationDirection={this.state.animationDirection}
                  activeEntity={this.state.selectedEntity}
                  retryCounter={this.retryCounter}
                  currentPage={this.state.currentPage}
                  activeFilter={this.state.filter}
                  filterOptions={this.filterOptions}
                  activeSort={this.state.sortObj}
                  searchText={this.state.query}
                  numColumns={this.state.numColumns}
                />
            }
            <Overview
              toggleOverview={!isNil(this.state.selectedEntity)}
              entity={this.state.selectedEntity}
              onClose={this.handleEntityClick.bind(this)}
              onCloseAndRefresh={this.onOverviewCloseAndRefresh.bind(this)}
            />
          </div>
        </Pagination>
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

export default EntityListView;
