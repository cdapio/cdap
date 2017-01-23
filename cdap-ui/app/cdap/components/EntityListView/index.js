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
import EntityCard from '../EntityCard';
import Pagination from 'components/Pagination';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import T from 'i18n-react';
const shortid = require('shortid');
const classNames = require('classnames');
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import MyUserStoreApi from 'api/userstore';
import PlusButtonStore from 'services/PlusButtonStore';
import globalEvents from 'services/global-events';

require('./EntityListView.scss');
import ee from 'event-emitter';

const defaultFilter = ['app', 'dataset', 'stream'];

// 312 = cardWith (300) + (5 x 2 side margins) + ( 1 x 2 border widths)
const cardWidthWithMarginAndBorder = 312;
// 140 = cardHeight (128) + (5 x 2 top bottom margins) + (1 x 2 border widths)
const cardHeightWithMarginAndBorder = 140;

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
        displayName: T.translate('commons.entity.program.plural'),
        id: 'program'
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
      isSortDisabled: false,
      query: '',
      isSearchDisabled: false,
      entities: [],
      selectedEntity: null,
      numPages: 1,
      loading: true,
      currentPage: 1,
      animationDirection: 'next',
      showSplash: true,
      userStoreObj : ''
    };

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
    this.setState({
      sortObj: this.sortOptions[4]
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
      queryObject.filter !== this.state.filter &&
      queryObject.sort.fullSort !== this.state.sortObj.fullSort &&
      queryObject.query !== this.state.query &&
      queryObject.page !== this.state.currentPage
    ) {
      this.updateData(queryObject.query, queryObject.filter, queryObject.sort, nextProps.params.namespace, queryObject.page);
      this.setState({
        filter: queryObject.filter,
        sortObj: queryObject.sort,
        query: queryObject.query,
        currentPage: queryObject.page,
        loading: true,
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

  calculatePageSize() {
    let entityListViewEle = document.getElementsByClassName('entity-list-view');

    if (!entityListViewEle.length) {
      return;
    }
    // Performs calculations to determine number of entities to render per page
    // minus 60px of padding in entity-list-view (30px each side)
    let containerWidth = entityListViewEle[0].offsetWidth - 60;

    // Subtract 55px to account for entity-list-header's height (30px) and container's top margin (25px)
    // minus 20px of padding from top and bottom (10px each)
    let containerHeight = document.getElementsByClassName('entity-list-view')[0].offsetHeight - 55 - 20;

    let numColumns = Math.floor(containerWidth / cardWidthWithMarginAndBorder);
    let numRows = Math.floor(containerHeight / cardHeightWithMarginAndBorder);

    // We must have one column and one row at the very least
    if (numColumns === 0) {
      numColumns = 1;
    }

    if (numRows === 0) {
      numRows = 1;
    }

    this.pageSize = numColumns * numRows;
  }
  // Retrieve entities for rendering
  componentDidMount() {
    this.calculatePageSize();
    this.updateData();
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

    this.setState({
      loading : true
    });

    let params = {
      namespace: namespace,
      target: filter,
      limit: this.pageSize,
      offset: offset
    };

    let isSortDisabled = typeof query === 'string' && query.length ? true : false;
    let isSearchDisabled = sortObj.fullSort === 'none' ? false : true;
    if (typeof query === 'string' && query.length) {
      params.query = `${query}*`;
    } else {
      params.sort = sortObj.fullSort === 'none' ? this.sortOptions[3].fullSort : sortObj.fullSort;
      params.query = '*';
      params.numCursors = 10;
    }
    let total;
    MySearchApi.search(params)
      .map((res) => {
        total = res.total;
        return res.results
          .map(parseMetadata)
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          });
      })
      .subscribe((res) => {
        this.setState({
          isSortDisabled,
          isSearchDisabled,
          entities: res,
          loading: false,
          entityErr: false,
          numPages: Math.ceil(total / this.pageSize)
        });
      }, (err) => {
        // On Error: render page as if there are no results found
        this.setState({
          isSortDisabled,
          isSearchDisabled,
          loading : false,
          entityErr : typeof err === 'object' ? err.response : err
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
      filter : filters
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
      animationDirection : direction
    }, () => this.search());
  }

  handleSortClick(option) {
    let isSearchDisabled = option.fullSort === 'none' ? false : true;
    this.setState({
      sortObj : option,
      isSearchDisabled
    }, () => {
      this.search(this.state.query, this.state.filter, option);
    });
  }

  handleSearch(query) {
    let isSortDisabled = typeof query === 'string' && query.length ? true : false;
    this.setState({
      query,
      isSortDisabled,
      sortObj: this.sortOptions[0]
    }, () => {
      this.search(query, this.state.filter, this.state.sortObj);
    });
  }

  handleEntityClick(uniqueId) {
    this.setState({selectedEntity: uniqueId});
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
      animationDirection : direction
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

  render() {

    if (!this.state.showSplash) {
      return (
        <div className="splash-screen-container">
          <div className="splash-screen-first-time">
            <h2 className="welcome-message">
              {T.translate('features.EntityListView.SplashScreen.welcomeMessage')}
            </h2>
            <div className="beta-notice">
              {T.translate('features.EntityListView.SplashScreen.welcomeMessage1')}*
            </div>
            <div className="cdap-fist-icon">
              <span className="icon-fist" />
            </div>
            <div className="introducing">
              {T.translate('features.EntityListView.SplashScreen.introText')}
            </div>
            <div className="app-store-bd">
              {T.translate('features.EntityListView.SplashScreen.introText1')}
            </div>
            <div
              className="splash-screen-first-time-btn"
              onClick={this.openMarketModal.bind(this)}
            >
              <span className="icon-CaskMarket" />
              {T.translate('features.EntityListView.SplashScreen.caskmarket')}
            </div>
            <div
              className="go-to-cdap"
              onClick={this.dismissSplash.bind(this)}
            >
              {T.translate('features.EntityListView.SplashScreen.gotoLabel')}
            </div>
            <div className="splash-screen-disclaimer">
              <p>
                * {T.translate('features.EntityListView.SplashScreen.disclaimerMessage')}
              </p>
            </div>
          </div>
        </div>
      );
    }


    const empty = (
      <h3 className="text-xs-center empty-message">
        {T.translate('features.EntityListView.emptyMessage')}
      </h3>
    );

    const errorContainer = (
      <h3 className="text-xs-center empty-message text-danger">
        <span className="fa fa-exclamation-triangle"></span>
        <span>{this.state.entityErr}</span>
      </h3>
    );

    const loading = (
      <h3 className="text-xs-center">
        <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
      </h3>
    );

    let entitiesToBeRendered;
    let bodyContent;

    if (this.state.loading) {
      bodyContent = loading;
    } else if (this.state.entities.length === 0) {
      entitiesToBeRendered = this.state.entityErr ? errorContainer : empty;

      bodyContent = (
        <div className="entities-container">
          {entitiesToBeRendered}
        </div>
      );

    } else {
      bodyContent = this.state.entities.map(
        (entity) => {
          return (
            <EntityCard
              className={
                classNames('entity-card-container',
                  { active: entity.uniqueId === this.state.selectedEntity }
                )
              }
              activeEntity={this.state.selectedEntity}
              key={entity.uniqueId}
              onClick={this.handleEntityClick.bind(this, entity.uniqueId)}
              entity={entity}
              onUpdate={this.search.bind(this)}
            />
          );
        }
      );
    }

    return (
      <div>
        <EntityListHeader
          filterOptions={this.filterOptions}
          onFilterClick={this.handleFilterClick.bind(this)}
          activeFilter={this.state.filter}
          sortOptions={this.sortOptions}
          isSortDisabled={this.state.isSortDisabled}
          activeSort={this.state.sortObj}
          onSortClick={this.handleSortClick.bind(this)}
          onSearch={this.handleSearch.bind(this)}
          isSearchDisabled={this.state.isSearchDisabled}
          searchText={this.state.query}
          numberOfPages={this.state.numPages}
          currentPage={this.state.currentPage}
          onPageChange={this.handlePageChange}
        />
        <Pagination
          className="entity-list-view"
          setCurrentPage={this.handlePageChange}
          totalPages={this.state.numPages}
          currentPage={this.state.currentPage}
          setDirection={this.setAnimationDirection}
        >
          <div className="entities-container">
            <ReactCSSTransitionGroup
              component="div"
              transitionName={"entity-animation--" + this.state.animationDirection}
              transitionEnterTimeout={1000}
              transitionLeaveTimeout={1000}
            >
              {bodyContent}
            </ReactCSSTransitionGroup>
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
