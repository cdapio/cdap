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

import React, {Component} from 'react';
import {MySearchApi} from '../../../api/search';
import {parseMetadata} from '../../../services/metadata-parser';
import Card from '../../common/Card';
import HomeHeader from './HomeHeader';
require('./Home.less');

export default class Home extends Component {
  constructor(props) {
    super(props);

    this.filterOptions = [
      { displayName: 'Applications', id: 'app' },
      { displayName: 'Artifacts', id: 'artifact' },
      { displayName: 'Programs', id: 'program' },
      { displayName: 'Datasets', id: 'dataset' },
      { displayName: 'Streams', id: 'stream' },
      { displayName: 'Views', id: 'view' },
    ];

    this.state = {
      filter: ['artifact', 'app', 'dataset', 'stream', 'view'],
      sort: '',
      entities: []
    };

    this.search();
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
                  <h4>Type: {entity.type}</h4>
                </Card>
              );
            })
          }
        </div>

      </div>
    );
  }
}
