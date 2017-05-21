/*
 * Copyright © 2017 Cask Data, Inc.
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
import DataPrepBrowser from 'components/DataPrep/DataPrepBrowser';
import {setActiveBrowser, setDatabaseProperties} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {objectQuery} from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';

export default class DatabaseBrowserWrapper extends Component {
  constructor(props) {
    super(props);

    this.state = {
      id: null,
      loading: true,
      error: null
    };
  }

  componentWillMount() {
    this.fetchConnectionInfo(this.props.databaseId);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.databaseId === this.state.id) { return; }
    this.fetchConnectionInfo(nextProps.databaseId);
  }

  fetchConnectionInfo(id) {
    if (!id) { return; }

    this.setState({loading: true});

    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      connectionId: id
    };

    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        let properties = objectQuery(res, 'values', 0, 'properties');

        setActiveBrowser({ name: 'database' });
        setDatabaseProperties({
          properties
        });

        this.setState({
          loading: false,
          error: null,
          id
        });
      }, (err) => {
        console.log('Error fetching connection', err);

        this.setState({
          loading: false,
          error: JSON.stringify(err)  // still unsure what the error looks like
        });
      });
  }

  render() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }

    if (this.state.error) {
      return (
        <div>
          <h4 className="text-xs-center text-danger">
            {this.state.error}
          </h4>
        </div>
      );
    }

    return (
      <DataPrepBrowser toggle={this.props.toggle} />
    );
  }
}

DatabaseBrowserWrapper.propTypes = {
  databaseId: PropTypes.string,
  toggle: PropTypes.func
};
