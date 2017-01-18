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

import React, { Component, PropTypes } from 'react';
import NamespaceStore from 'services/NamespaceStore';
import myExploreApi from 'api/explore';
import shortid from 'shortid';
import sortBy from 'lodash/sortBy';
import TableItem from 'wrangler/components/Explore/TableItem';
import T from 'i18n-react';

require('./Explore.scss');

export default class Explore extends Component {
  constructor(props) {
    super(props);

    this.state = {
      list: [],
      isExpanded: false
    };

    this.renderTable = this.renderTable.bind(this);
    this.toggleExpanded = this.toggleExpanded.bind(this);
  }

  componentWillMount() {
    myExploreApi.fetchTables({
      namespace: NamespaceStore.getState().selectedNamespace
    }).subscribe((res) => {
      let list = res.map((exploreTable) => {
        let table = exploreTable.table.split('_');
        return {
          id: shortid.generate(),
          type: table[0],
          name: table.slice(1).join('_')
        };
      }).filter((exploreTable) => {
        return exploreTable.name.charAt(0) !== '_';
      });

      this.setState({
        list: sortBy(list, ['name'])
      });
    });
  }

  toggleExpanded() {
    this.setState({isExpanded: !this.state.isExpanded});
  }

  renderTable(table) {
    return (
      <TableItem
        key={table.id}
        table={table}
        wrangle={this.props.wrangle}
      />
    );
  }

  render() {
    let tableList = this.state.list;
    const VIEW_LIMIT = 18;

    if (!this.state.isExpanded) {
      tableList = tableList.slice(0, VIEW_LIMIT);
    }

    const showMoreLink = (
      <div className="text-xs-right">
        <a
          href="#"
          onClick={this.toggleExpanded}
        >
          {this.state.isExpanded ? 'Show Less' : 'Show More'}
        </a>
      </div>
    );


    return (
      <div className="wrangler-explore-container">
        <h4 className="text-xs-center">
          {T.translate('features.Wrangler.Explore.browse', {count: this.state.list.length})}
        </h4>
        <div className="text-xs-center">
          {tableList.map(this.renderTable)}
        </div>

        {this.state.list.length > VIEW_LIMIT ? showMoreLink : null}
      </div>
    );
  }
}

Explore.propTypes = {
  wrangle: PropTypes.func
};
