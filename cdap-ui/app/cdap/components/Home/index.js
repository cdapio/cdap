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
import Card from '../Card';
import HomeHeader from './HomeHeader';
import T from 'i18n-react';
import Store from '../../services/store/store.js';

require('./Home.less');

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
        displayName: T.translate('commons.entity.program.plural'),
        id: 'program'
      },
      {
        displayName: T.translate('commons.entity.dataset.plural'),
        id: 'dataset'
      },
      {
        displayName: T.translate('commons.entity.stream.plural'),
        id: 'stream'
      },
      {
        displayName: T.translate('commons.entity.view.plural'),
        id: 'view'
      },
    ];

    this.state = {
      filter: ['artifact', 'app', 'dataset', 'stream', 'view'],
      sort: '',
      entities: []
    };

    this.search();
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

  componentWillUpdate(nextProps) {
    if(this.doesNsExist(nextProps.params.namespace)){
      Store.dispatch({
        type : 'SELECT_NAMESPACE',
        payload : {
          selectedNamespace : nextProps.params.namespace
        }
      });
    } else {
      //FIXME: Must redirect if passed invalid namespace
    }
  }

  search(filter) {
    let params = {
      namespace: 'default',
      query: '*',
      target: filter || this.state.filter
    };

    MySearchApi.search(params)
      .map((res) => {
        return res.map(parseMetadata);
      })
      .subscribe(
        (res) => {
          this.setState({entities: res});
        }
      );
  }

  handleFilterClick(option) {
    let arr = [...this.state.filter];
    if (this.state.filter.includes(option.id)) {
      let index = arr.indexOf(option.id);
      arr.splice(index, 1);
    } else {
      arr.push(option.id);
    }

    this.search(arr);
    this.setState({filter: arr});
  }

  render() {

    return (
      <div>
        <HomeHeader
          filterOptions={this.filterOptions}
          onFilterClick={this.handleFilterClick.bind(this)}
          activeFilter={this.state.filter}
        />
        <div className="entity-list">
          {this.state.entities.map(
            (entity, index) => {
              return (
                <Card
                  key={index}
                  title={entity.id}
                  cardClass='home-cards'
                >
                  <h4>
                    <span>{T.translate('features.Home.Cards.type')}</span>
                    <span>{entity.type}</span>
                  </h4>
                </Card>
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
