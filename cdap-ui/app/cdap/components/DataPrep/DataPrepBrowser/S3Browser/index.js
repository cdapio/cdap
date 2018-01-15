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
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {setS3Loading} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import BucketDataView from 'components/DataPrep/DataPrepBrowser/S3Browser/BucketData';
import {Route, Switch} from 'react-router-dom';
import Page404 from 'components/404';
import {Provider} from 'react-redux';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import S3Path from 'components/DataPrep/DataPrepBrowser/S3Browser/S3Path';
import ListingInfo from 'components/DataPrep/DataPrepBrowser/S3Browser/ListingInfo';
import S3Search from 'components/DataPrep/DataPrepBrowser/S3Browser/S3Search';
import classnames from 'classnames';

require('./S3Browser.scss');

const PREFIX = 'features.DataPrep.DataPrepBrowser.S3Browser';
export default class S3Browser extends Component {
  static propTypes = {
    toggle: PropTypes.func,
    location: PropTypes.object,
    match: PropTypes.object,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func
  };

  static defaultProps = {
    enableRouting: true
  };

  onWorkspaceCreate = (file) => {
    const {selectedNamespace: namespace} = NamespaceStore.getState();
    const {connectionId, prefix} = DataPrepBrowserStore.getState().s3;
    const strippedPrefix = prefix.slice(prefix.indexOf('/') + 1);
    const activeBucket = strippedPrefix.slice(0, strippedPrefix.indexOf('/'));
    setS3Loading();
    let headers = {
      'Content-Type': file.type
    };
    MyDataPrepApi
      .readS3File({
        namespace,
        connectionId,
        activeBucket,
        key: file.path,
        lines: 10000,
        sampler: 'first'
      }, null, headers)
      .subscribe(
        res => {
          let {id: workspaceId} = res.values[0];
          if (this.props.enableRouting) {
            window.location.href = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
          }
          if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
            this.props.onWorkspaceCreate(workspaceId);
          }
        }
      );
  };

  renderContentBody = () => {
    const BASEPATH = '/ns/:namespace/connections/s3/:s3Id';
    if (this.props.enableRouting) {
      return (
        <Switch>
          <Route
            path={`${BASEPATH}`}
            render={() => {
              return <BucketDataView {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
            }}
          />
          <Route render={Page404} />
        </Switch>
      );
    }
    return <BucketDataView {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
  };

  render() {
    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="s3-browser">
          <div className="top-panel">
            <div className="title">
              <h5>
                <span
                  className="fa fa-fw"
                  onClick={this.props.toggle}
                >
                  <IconSVG name="icon-bars" />
                </span>

                <span>
                  {T.translate(`${PREFIX}.TopPanel.selectData`)}
                </span>
              </h5>
            </div>
          </div>
          <div className={classnames("sub-panel", {'routing-disabled': !this.props.enableRouting})}>
            <div className="path-container">
              <S3Path
                baseStatePath={this.props.match.url}
                enableRouting={this.props.enableRouting}
              />
            </div>
            <div className="info-container">
              <span className="info">
                <ListingInfo />
              </span>
              <div className="search-container">
                <S3Search />
              </div>
            </div>
          </div>
          <div className="s3-content">
            {this.renderContentBody()}
          </div>
        </div>
      </Provider>
    );
  }
}
