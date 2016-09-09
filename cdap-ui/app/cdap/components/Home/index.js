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
import {MySearchApi} from '../../api/search';
import {parseMetadata} from '../../services/metadata-parser';
import Card from '../Card';
import HomeHeader from './HomeHeader';
require('./Home.less');

export default class Home extends Component {
  constructor(props) {
    super(props);

    this.state = {
      filter: ['artifact', 'app', 'dataset', 'program', 'stream', 'view'],
      sort: '',
      entities: []
    };

    let params = {
      namespace: 'default',
      query: '*',
      target: this.state.filter
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
  render() {
    return (
      <div>
        <HomeHeader />

        <div className="entity-list">
          {this.state.entities.map(
            (entity) => {
              return (
                <Card
                  key={entity.id}
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
