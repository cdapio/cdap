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
      // {
      //   displayName: T.translate('commons.entity.program.plural'),
      //   id: 'program'
      // },
      {
        displayName: T.translate('commons.entity.dataset.plural'),
        id: 'dataset'
      },
      {
        displayName: T.translate('commons.entity.stream.plural'),
        id: 'stream'
      }
      // {
      //   displayName: T.translate('commons.entity.view.plural'),
      //   id: 'view'
      // },
    ];
    this.sortOptions = [
      {
        displayName: T.translate('features.Home.Header.sortOptions.nameAsc'),
        sort: 'name asc'
      },
      {
        displayName: T.translate('features.Home.Header.sortOptions.nameDesc'),
        sort: 'name desc'
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
    this.makeSortFilterParams = this.makeSortFilterParams.bind(this);
    this.processQueryString = this.processQueryString.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    this.search(this.state.query, this.state.filter, this.state.sortObj, nextProps.params.namespace);
  }

  processQueryString() {
    let filtersArr = [];
    let sortTerm = '';
    let searchTerm = '';
    let sortOpt;

    if(this.props.location.query){
      filtersArr = this.props.location.query.filter ? this.props.location.query.filter.split(',') : [];
      sortTerm = this.props.location.query.sort ? this.props.location.query.sort : '';
      searchTerm = this.props.location.query.search ? this.props.location.query.search : '';
    }

    sortOpt = this.sortOptions.filter((option) => {
      return option.sort === sortTerm;
    });

    return {
      'filter' : filtersArr,
      'sort' : sortOpt[0],
      'search' : searchTerm
    };
  }

  componentDidMount() {
    Store.dispatch({
      type: 'SELECT_NAMESPACE',
      payload: {
        selectedNamespace: this.props.params.namespace
      }
    });

    //Parse URL and apply filters / sort
    let filterSortObj = this.processQueryString();
    let urlFilters;
    let urlSort;
    let urlSearch;

    if(typeof filterSortObj.filter !== 'undefined' && filterSortObj.filter.length === 0){
      urlFilters = this.state.filter;
    } else {
      urlFilters = filterSortObj.filter;
    }

    urlSearch = filterSortObj.search;
    urlSort = filterSortObj.sort;

    this.search(urlSearch, urlFilters, urlSort);
  }

  search(
    query = this.state.query,
    filter = this.state.filter,
    sortObj = this.state.sortObj,
    namespace = this.props.params.namespace
  ) {

    if (filter.length === 0) {
      this.setState({query, filter, sortObj, entities: [], selectedEntity: null, loading: false});
      return;
    }

    this.setState({loading: true});

    let params = {
      namespace: namespace,
      query: `${query}*`,
      target: filter,
      sort: sortObj.sort
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
    let arr = [...this.state.filter];
    if (this.state.filter.includes(option.id)) {
      let index = arr.indexOf(option.id);
      arr.splice(index, 1);
    } else {
      arr.push(option.id);
    }

    this.search(this.state.query, arr, this.state.sortObj);
  }

  handleSortClick(option) {
    this.search(this.state.query, this.state.filter, option);
  }

  handleSearch(query) {
    this.search(query, this.state.filter, this.state.sortObj);
  }

  handleEntityClick(uniqueId) {
    this.setState({selectedEntity: uniqueId});
  }

  makeSortFilterParams(){
    let sortAndFilterParams = '';
    let sortParams = '';
    let filterParams = '';
    let searchParams = '';
    let queryParams = [];
    let searchTerm = this.state.query;

    if(this.state.sortObj.sort){
      sortParams = 'sort=' + this.state.sortObj.sort.split(' ').join('+');
    }
    filterParams = 'filter=' + this.state.filter.join(',');

    if(searchTerm.length > 0){
      searchParams = 'search=' + searchTerm;
    }

    queryParams = [sortParams, filterParams, searchParams].filter((element) => {
      return element.length > 0;
    });

    sortAndFilterParams = queryParams.join('&');

    if(sortAndFilterParams.length > 0){
      sortAndFilterParams = '?' + sortAndFilterParams;
    }

    let obj = {
      title: 'CDAP',
      url: location.pathname + sortAndFilterParams
    };

    history.pushState(obj, obj.title, obj.url);
  }

  render() {

    this.makeSortFilterParams();

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
