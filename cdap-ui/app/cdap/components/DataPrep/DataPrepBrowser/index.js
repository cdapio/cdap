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
import DataPrepErrorBanner from 'components/DataPrep/DataPrepBrowser/ErrorBanner';
import {Provider} from 'react-redux';

const browserMap = {
  database: DatabaseBrowser,
  file: FileBrowser,
  kafka: KafkaBrowser,
  s3: S3Browser,
  gcs: GCSBrowser,
  bigquery: BigQueryBrowser
};

export default class DataPrepBrowser extends Component {
  state = {
    activeBrowser: DataPrepBrowserStore.getState().activeBrowser
  };

  componentDidMount() {
    this.storeSubscription = DataPrepBrowserStore.subscribe(() => {
      let {activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name && this.state.activeBrowser.name !== activeBrowser.name) {
        this.setState({
          activeBrowser
        });
      }
    });
    if (typeof this.props.setActiveBrowser === 'function') {
      this.props.setActiveBrowser();
    }
  }

  componentDidUpdate() {
    if (typeof this.props.setActiveBrowser === 'function') {
      this.props.setActiveBrowser();
    }
  }

  componentWillUnmount() {
    if (typeof this.storeSubscription === 'function') {
      this.storeSubscription();
    }
  }

  render() {
    let activeBrowser = this.state.activeBrowser.name.toLowerCase();
    if (browserMap.hasOwnProperty(activeBrowser)) {
      let Tag = browserMap[activeBrowser];
      return (
        <Provider store={DataPrepBrowserStore}>
          <React.Fragment>
            <DataPrepErrorBanner />
            <Tag
              {...this.props}
            />
          </React.Fragment>
        </Provider>
      );
    }

    return null; // FIXME: Should this be 404? Would we even end up in this state?
  }
}
DataPrepBrowser.propTypes = {
  location: PropTypes.object,
  match: PropTypes.object,
  setActiveBrowser: PropTypes.func
};
