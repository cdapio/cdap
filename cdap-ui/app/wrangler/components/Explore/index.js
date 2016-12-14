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

require('./Explore.less');

export default class Explore extends Component {
	constructor(props) {
		super(props);

		this.state = {
			list: []
		};

		this.renderTable = this.renderTable.bind(this);
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
			});

			this.setState({
				list: sortBy(list, ['name'])
			});
		});
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
		return (
			<div className="wrangler-explore-container">
				<h4 className="text-center">Browse Dataset</h4>
				<div className="text-center">
					{this.state.list.map(this.renderTable)}
				</div>
			</div>
		);
	}
}

Explore.propTypes = {
	wrangle: PropTypes.func
};
