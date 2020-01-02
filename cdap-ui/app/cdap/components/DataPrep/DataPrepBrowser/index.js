/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import DatabaseBrowser from 'components/DataPrep/DataPrepBrowser/DatabaseBrowser';
import FileBrowser from 'components/FileBrowser';
import S3Browser from 'components/DataPrep/DataPrepBrowser/S3Browser';
import KafkaBrowser from 'components/DataPrep/DataPrepBrowser/KafkaBrowser';
import GCSBrowser from 'components/DataPrep/DataPrepBrowser/GCSBrowser';
import BigQueryBrowser from 'components/DataPrep/DataPrepBrowser/BigQueryBrowser';
import SpannerBrowser from 'components/DataPrep/DataPrepBrowser/SpannerBrowser';
import ADLSBrowser from 'components/DataPrep/DataPrepBrowser/ADLSBrowser';
import DataPrepErrorBanner from 'components/DataPrep/DataPrepBrowser/ErrorBanner';
import { Provider } from 'react-redux';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';

const browserMap = {
  [ConnectionType.DATABASE]: DatabaseBrowser,
  [ConnectionType.FILE]: FileBrowser,
  [ConnectionType.KAFKA]: KafkaBrowser,
  [ConnectionType.S3]: S3Browser,
  [ConnectionType.GCS]: GCSBrowser,
  [ConnectionType.BIGQUERY]: BigQueryBrowser,
  [ConnectionType.SPANNER]: SpannerBrowser,
  [ConnectionType.ADLS]: ADLSBrowser,
};

export default class DataPrepBrowser extends Component {
  state = {
    activeBrowser: DataPrepBrowserStore.getState().activeBrowser,
  };

  componentDidMount() {
    this.storeSubscription = DataPrepBrowserStore.subscribe(() => {
      let { activeBrowser } = DataPrepBrowserStore.getState();
      if (activeBrowser.name && this.state.activeBrowser.name !== activeBrowser.name) {
        this.setState({
          activeBrowser,
        });
      }
    });
    if (typeof this.props.setActiveConnection === 'function') {
      this.props.setActiveConnection();
    }
  }

  componentDidUpdate() {
    // TODO: Right now the burden of checking whether the connection id is actually
    // changed falls into the implementation of the various 'setActiveConnection'
    // functions for each connection type (e.g. setS3AsActiveBrowser). If the connection
    // changes, we dispatch an API call to get the connection details, then fetch the current
    // bucket details. If the connection ID didn't change i.e. user clicks on a bucket
    // in the same connection, then we just fetch the bucket details directly.
    // Ideally, these two calls should be in separate functions, and the checking
    // of whether the connection ID changed or not should happen here.
    // JIRA: CDAP-14173
    if (typeof this.props.setActiveConnection === 'function') {
      this.props.setActiveConnection();
    }
  }

  componentWillUnmount() {
    if (typeof this.storeSubscription === 'function') {
      this.storeSubscription();
    }
  }

  render() {
    let activeBrowser = this.state.activeBrowser.name;
    if (Object.prototype.hasOwnProperty.call(browserMap, activeBrowser)) {
      let Tag = browserMap[activeBrowser];
      return (
        <Provider store={DataPrepBrowserStore}>
          <React.Fragment>
            <DataPrepErrorBanner />
            <Tag {...this.props} />
          </React.Fragment>
        </Provider>
      );
    }

    return null; // FIXME: Should this be 404? Would we even end up in this state?
  }
}
DataPrepBrowser.propTypes = {
  match: PropTypes.object,
  setActiveConnection: PropTypes.func,
};
