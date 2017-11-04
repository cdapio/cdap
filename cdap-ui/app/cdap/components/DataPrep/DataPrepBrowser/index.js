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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import DatabaseBrowser from 'components/DataPrep/DataPrepBrowser/DatabaseBrowser';
import FileBrowser from 'components/FileBrowser';
import S3Browser from 'components/DataPrep/DataPrepBrowser/S3Browser';
import KafkaBrowser from 'components/DataPrep/DataPrepBrowser/KafkaBrowser';
import GCSBrowser from 'components/DataPrep/DataPrepBrowser/GCSBrowser';

const browserMap = {
  database: DatabaseBrowser,
  file: FileBrowser,
  kafka: KafkaBrowser,
  s3: S3Browser,
  gcs: GCSBrowser
};

export default class DataPrepBrowser extends Component {
  constructor(props) {
    super(props);
    let store = DataPrepBrowserStore.getState();
    this.state = {
      activeBrowser: store.activeBrowser
    };
  }
  componentWillMount() {
    DataPrepBrowserStore.subscribe(() => {
      let {activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name && this.state.activeBrowser.name !== activeBrowser.name) {
        this.setState({
          activeBrowser
        });
      }
    });
  }
  render() {
    let activeBrowser = this.state.activeBrowser.name.toLowerCase();
    if (browserMap.hasOwnProperty(activeBrowser)) {
      let Tag = browserMap[activeBrowser];
      return (
        <Tag
          {...this.props}
        />
      );
    }

    return null; // FIXME: Should this be 404? Would we even end up in this state?
  }
}
DataPrepBrowser.propTypes = {
  location: PropTypes.object,
  match: PropTypes.object
};
