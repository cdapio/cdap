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
import {MySearchApi} from '../../api/search';
import {parseMetadata} from '../../services/metadata-parser';
import HomeHeader from './HomeHeader';
import EntityCard from '../EntityCard';
import Store from 'services/store/store.js';
import T from 'i18n-react';
const shortid = require('shortid');
const classNames = require('classnames');
require('./Home.less');

const defaultFilter = ['app', 'dataset', 'stream'];

class Home extends Component {
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
    this.sortOptions = [
      {
        displayName: T.translate('features.Home.Header.sortOptions.nameAsc.displayName'),
        sort: T.translate('features.Home.Header.sortOptions.nameAsc.sortField'),
        order: T.translate('features.Home.Header.sortOptions.nameAsc.sortOrder'),
        fullSort: T.translate('features.Home.Header.sortOptions.nameAsc.fullSort')
      },
      {
        displayName: T.translate('features.Home.Header.sortOptions.nameDesc.displayName'),
        sort: T.translate('features.Home.Header.sortOptions.nameDesc.sortField'),
        order: T.translate('features.Home.Header.sortOptions.nameDesc.sortOrder'),
        fullSort: T.translate('features.Home.Header.sortOptions.nameDesc.fullSort')
      }
    ];
    this.state = {
      filter: defaultFilter,
      sortObj: this.sortOptions[0],
      query: '',
      entities: [],
      selectedEntity: null,
      loading: true
    };
    this.updateQueryString = this.updateQueryString.bind(this);
    this.getQueryObject = this.getQueryObject.bind(this);
  }
  componentWillReceiveProps(nextProps) {
    this.search(this.state.query, this.state.filter, this.state.sortObj, nextProps.params.namespace);
  }

  //Update Store and State to correspond to query parameters before component renders
  componentWillMount() {
    Store.dispatch({
      type: 'SELECT_NAMESPACE',
      payload: {
        selectedNamespace: this.props.params.namespace
      }
    });

    let queryObject = this.getQueryObject();
    this.setState({
      filter: queryObject.filter,
      sortObj: queryObject.sort,
      query: queryObject.query
    });
  }

  //Retrieve entities for rendering
  componentDidMount(){
    this.search();
  }

  //Construct and return query object from query parameters
  getQueryObject() {
    let filters = '';
    let sortBy = '';
    let orderBy = '';
    let searchTerm = '';
    let sortOption = '';
    let query = this.props.location.query;

    //Get filters, order, sort, search from query
    if(query){
      orderBy = typeof query.order === 'string' ? query.order : '';
      sortBy = typeof query.sort === 'string' ? query.sort : '';
      searchTerm = typeof query.q === 'string' ? query.q : '';
      if(typeof query.filter === 'string'){
        filters = [query.filter];
      } else if(Array.isArray(query.filter)){
        filters = query.filter;
      }
    }

    //Ensure sort combination is valid
    sortOption = this.sortOptions.filter((option) => {
      return ( sortBy === option.sort && orderBy === option.order);
    });

    //If query parameters are valid return those else return the current state's parameters
    return ({
      'query' : searchTerm ? searchTerm : this.state.query,
      'sort' : sortOption.length === 0 ? this.state.sortObj : sortOption[0],
      'filter' : filters ? filters : this.state.filter
    });
  }

  search(
    query = this.state.query,
    filter = this.state.filter,
    sortObj = this.state.sortObj,
    namespace = this.props.params.namespace
  ) {
    //No entity types requested - set state and return
    if (filter.length === 0) {
      this.setState({query, filter, sortObj, entities: [], selectedEntity: null, loading: false});
      return;
    }

    this.setState({loading: true});

    let params = {
      namespace: namespace,
      query: `${query}*`,
      target: filter,
      sort: sortObj.fullSort
    };

    MySearchApi.search(params)
      .map((res) => {
        return res.results
          .map(parseMetadata)
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          })
          .filter((entity) => entity.id.charAt(0) !== '_');
      })
      .subscribe((res) => {
        this.setState({query, filter, sortObj, entities: res, selectedEntity: null, loading: false});
      });
  }

  handleFilterClick(option) {
    let filters = [...this.state.filter];
    if (this.state.filter.includes(option.id)) {
      let index = filters.indexOf(option.id);
      filters.splice(index, 1);
    } else {
      filters.push(option.id);
    }

    this.setState({
      filter : filters
    });

    this.search(this.state.query, filters, this.state.sortObj);
  }

  handleSortClick(option) {
    this.setState({
      sortObj : option
    });
    this.search(this.state.query, this.state.filter, option);
  }

  handleSearch(query) {
    this.search(query, this.state.filter, this.state.sortObj);
  }

  handleEntityClick(uniqueId) {
    this.setState({selectedEntity: uniqueId});
  }

  //Set query string using application state
  updateQueryString(){
    let queryString = '';
    let sort = '';
    let filter = '';
    let query = '';
    let queryParams = [];

    //Sort Params
    if(this.state.sortObj.sort){
      sort = 'sort=' + this.state.sortObj.sort + '&order=' + this.state.sortObj.order;
    }

    //Filter Params
    if(this.state.filter.length === 1){
      filter = 'filter=' + this.state.filter[0];
    } else if (this.state.filter.length > 1){
      filter = 'filter=' + this.state.filter.join('&filter=');
    }

    //Search Param
    if(this.state.query.length > 0){
      query = 'q=' + this.state.query;
    }

    //Combine Query Parameters
    queryParams = [query, sort, filter].filter((element) => {
      return element.length > 0;
    });
    queryString = queryParams.join('&');

    if(queryString.length > 0){
      queryString = '?' + queryString;
    }

    let obj = {
      title: 'CDAP',
      url: location.pathname + queryString
    };

    history.pushState(obj, obj.title, obj.url);
  }

  render() {
    this.updateQueryString();

    const empty = (
      <h3 className="text-center empty-message">
        {T.translate('features.Home.emptyMessage')}
      </h3>
    );

    const loading = (
      <h3 className="text-center">
        <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
      </h3>
    );

    let entitiesToBeRendered;
    if(this.state.loading){
      entitiesToBeRendered = loading;
    } else if(this.state.entities.length === 0) {
      entitiesToBeRendered = empty;
    } else {
      entitiesToBeRendered = this.state.entities.map(
        (entity) => {
          return (
            <div
              className={
                classNames('entity-card-container',
                  { active: entity.uniqueId === this.state.selectedEntity }
                )
              }
              key={entity.uniqueId}
              onClick={this.handleEntityClick.bind(this, entity.uniqueId)}
            >
              <EntityCard
                entity={entity}
                onUpdate={this.search.bind(this)}
              />
            </div>
          );
        }
      );
    }

    return (
      <div>
        <HomeHeader
          filterOptions={this.filterOptions}
          onFilterClick={this.handleFilterClick.bind(this)}
          activeFilter={this.state.filter}
          sortOptions={this.sortOptions}
          activeSort={this.state.sortObj}
          onSortClick={this.handleSortClick.bind(this)}
          onSearch={this.handleSearch.bind(this)}
          searchText={this.state.query}
        />
        <div className="entity-list">
          {entitiesToBeRendered}
        </div>
      </div>
    );
  }
}

Home.propTypes = {
  params: PropTypes.shape({
    namespace : PropTypes.string
  }),
  location: PropTypes.object,
  history: PropTypes.object
};

export default Home;
