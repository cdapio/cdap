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
import MarketStore from 'components/Market/store/market-store.js';
import Fuse from 'fuse.js';
import MarketPlaceUsecaseEntity from 'components/MarketPlaceUsecaseEntity';
require('./UsecaseTab.less');

export default class UsecaseTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entities: this.getUsecases()
    };
  }
  getUsecases() {
    const {list, filter} = MarketStore.getState();
    if (filter !== 'usecase') {
      return;
    }

    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: [
        "categories"
      ]
    };

    let fuse = new Fuse(list, fuseOptions);
    return fuse.search(filter);
  }
  render() {
    return (
      <div className="usecase-content-tab">
        {
          this.state
            .entities
            .map( entity => (
              <MarketPlaceUsecaseEntity
                key={entity.id}
                entity={entity}
                entityId={entity.id}
                beta={entity.beta}
              />
            ))
        }
      </div>
    );
  }

}
