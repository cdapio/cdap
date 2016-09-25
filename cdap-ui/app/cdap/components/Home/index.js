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
import T from 'i18n-react';
import Store from '../../services/store/store.js';

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
      entities: [],
      selectedEntity: null,
      loading: true
    };

    this.doesNsExist = this.doesNsExist.bind(this);
  }

  findNamespace(){
    let namespaces = Store.getState().namespaces;
    let name = this.props.params.namespace;

    let result = _.find(namespaces, { 'name' : name });
    return result ? true : false;
  }

  doesNsExist(ns){
    let namespaces = Store.getState().namespaces.map(function(item){
      return item.name;
    });
    return namespaces.indexOf(ns) !== -1;
  }

  componentWillMount(){
    if(this.findNamespace()){
      Store.dispatch({
        type : 'SELECT_NAMESPACE',
        payload : {
          selectedNamespace : this.props.params.namespace
        }
      });
    } else {
      //FIXME: Must redirect if passed invalid namespace
    }
  }

  componentDidMount() {
    this.search();

    Store.subscribe(() => {
      this.search();
    });
  }

  search(query = '', filter = this.state.filter, sortObj = this.state.sortObj) {
    this.setState({loading: true});

    let params = {
      namespace: Store.getState().selectedNamespace,
      query: `${query}*`,
      target: filter,
      sortObj: sortObj.sort
    };

    MySearchApi.search(params)
      .map((res) => {
        return res.results
          .map(parseMetadata)
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          });
      })
      .subscribe((res) => {
        this.setState({filter, sortObj, entities: res, selectedEntity: null, loading: false});
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

  render() {
    const empty = (
      <h3 className="text-center">
        {T.translate('features.Home.emptyMessage')}
      </h3>
    );

    const loading = (
      <h3 className="text-center">
        <span className="fa fa-spinner fa-spin loading-spinner"></span>
      </h3>
    );

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
          {
            this.state.loading ? loading :
            this.state.entities.length === 0 ? empty :
            this.state.entities.map(
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
                  <EntityCard entity={entity} />
                </div>
              );
            })
          }
        </div>
      </div>
    );
  }
}

Home.propTypes = {
  params: PropTypes.shape({
    namespace : PropTypes.string
  })
};

export default Home;
