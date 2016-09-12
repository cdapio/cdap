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
import SearchTextBox from '../SearchTextBox';
import MarketPlaceEntity from '../MarketPlaceEntity';
import T from 'i18n-react';
require('./AllTabContents.less');

export default class AllTabContents extends Component {
  constructor(props) {
    super(props);
    this.state = {
      searchStr: ''
    };
  }
  onSearch(changeEvent) {
    // For now just save. Eventually we will make a backend call to get the search result.
    this.setState({searchStr: changeEvent.target.value});
  }
  render() {
    return (
      <div className="all-tab-content">
        <SearchTextBox
          placeholder={T.translate('features.Market.search-placeholder')}
          value={this.state.searchStr}
          onChange={this.onSearch.bind(this)}
        />
        <div className="body-section">
          {
            Array
              .apply(null, {length: 50})
              .map((e, index) => (
                <MarketPlaceEntity
                  name="Entity Name"
                  subtitle="Version: 1.0"
                  key={index}
                  size="medium" />
              ))
          }
        </div>
      </div>
    );
  }
}
